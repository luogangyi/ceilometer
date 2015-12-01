#
# Copyright 2014-2015 eNovance
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import copy
import fnmatch
from hashlib import sha1
import itertools
import operator
import os
import threading
import uuid

from oslo_config import cfg
from oslo_log import log
import six
from stevedore import extension

from ceilometer import declarative
from ceilometer import dispatcher
from nova.openstack.common import memorycache
from ceilometer.dispatcher import gnocchi_client
from ceilometer.i18n import _, _LE, _LW
from ceilometer import keystone_client

CACHE_NAMESPACE = uuid.UUID(bytes=sha1(__name__).digest()[:16])
LOG = log.getLogger(__name__)

dispatcher_opts = [
    cfg.BoolOpt('filter_service_activity',
                default=True,
                help='Filter out samples generated by Gnocchi '
                'service activity'),
    cfg.StrOpt('filter_project',
               default='gnocchi',
               help='Gnocchi project used to filter out samples '
               'generated by Gnocchi service activity'),
    cfg.StrOpt('url',
               default="http://localhost:8041",
               help='URL to Gnocchi.'),
    cfg.StrOpt('archive_policy',
               default=None,
               help='The archive policy to use when the dispatcher '
               'create a new metric.'),
    cfg.StrOpt('resources_definition_file',
               default='gnocchi_resources.yaml',
               help=_('The Yaml file that defines mapping between samples '
                      'and gnocchi resources/metrics')),
]

cfg.CONF.register_opts(dispatcher_opts, group="dispatcher_gnocchi")


def cache_key_mangler(key):
    """Construct an opaque cache key."""
    if six.PY2:
        key = key.encode('utf-8')
    return uuid.uuid5(CACHE_NAMESPACE, key).hex


def log_and_ignore_unexpected_workflow_error(func):
    def log_and_ignore(self, *args, **kwargs):
        try:
            func(self, *args, **kwargs)
        except gnocchi_client.UnexpectedError as e:
            LOG.error(six.text_type(e))
    return log_and_ignore


class ResourcesDefinitionException(Exception):
    def __init__(self, message, definition_cfg):
        super(ResourcesDefinitionException, self).__init__(message)
        self.definition_cfg = definition_cfg

    def __str__(self):
        return '%s %s: %s' % (self.__class__.__name__,
                              self.definition_cfg, self.message)


class ResourcesDefinition(object):

    MANDATORY_FIELDS = {'resource_type': six.string_types,
                        'metrics': list}

    def __init__(self, definition_cfg, default_archive_policy, plugin_manager):
        self._default_archive_policy = default_archive_policy
        self.cfg = definition_cfg

        for field, field_type in self.MANDATORY_FIELDS.items():
            if field not in self.cfg:
                raise declarative.DefinitionException(
                    _LE("Required field %s not specified") % field, self.cfg)
            if not isinstance(self.cfg[field], field_type):
                raise declarative.DefinitionException(
                    _LE("Required field %(field)s should be a %(type)s") %
                    {'field': field, 'type': field_type}, self.cfg)

        self._attributes = {}
        for name, attr_cfg in self.cfg.get('attributes', {}).items():
            self._attributes[name] = declarative.Definition(name, attr_cfg,
                                                            plugin_manager)

    def match(self, metric_name):
        for t in self.cfg['metrics']:
            if fnmatch.fnmatch(metric_name, t):
                return True
        return False

    def attributes(self, sample):
        attrs = {}
        for name, definition in self._attributes.items():
            value = definition.parse(sample)
            if value is not None:
                attrs[name] = value
        return attrs

    def metrics(self):
        metrics = {}
        for t in self.cfg['metrics']:
            archive_policy = self.cfg.get('archive_policy',
                                          self._default_archive_policy)
            if archive_policy is None:
                metrics[t] = {}
            else:
                metrics[t] = dict(archive_policy_name=archive_policy)
        return metrics


class GnocchiDispatcher(dispatcher.Base):
    """Dispatcher class for recording metering data into database.

    The dispatcher class records each meter into the gnocchi service
    configured in ceilometer configuration file. An example configuration may
    look like the following:

    [dispatcher_gnocchi]
    url = http://localhost:8041
    archive_policy = low

    To enable this dispatcher, the following section needs to be present in
    ceilometer.conf file

    [DEFAULT]
    meter_dispatchers = gnocchi
    """
    def __init__(self, conf):
        super(GnocchiDispatcher, self).__init__(conf)
        self.conf = conf
        self.filter_service_activity = (
            conf.dispatcher_gnocchi.filter_service_activity)
        self._ks_client = keystone_client.get_client()
        self.resources_definition = self._load_resources_definitions(conf)

        self.cache = None
        try:
            if cfg.CONF.memcached_servers:
                self.cache = memorycache.get_client()
        except Exception as e:
            LOG.warn(_LW('unable to configure memcache: %s') % e)

        self._gnocchi_project_id = None
        self._gnocchi_project_id_lock = threading.Lock()
        self._gnocchi_resource_lock = threading.Lock()

        self._gnocchi = gnocchi_client.Client(conf.dispatcher_gnocchi.url)

    # TODO(sileht): Share yaml loading with
    # event converter and declarative notification

    @staticmethod
    def _get_config_file(conf, config_file):
        if not os.path.exists(config_file):
            config_file = cfg.CONF.find_file(config_file)
        return config_file

    @classmethod
    def _load_resources_definitions(cls, conf):
        plugin_manager = extension.ExtensionManager(
            namespace='ceilometer.event.trait_plugin')
        data = declarative.load_definitions(
            {}, conf.dispatcher_gnocchi.resources_definition_file)
        return [ResourcesDefinition(r, conf.dispatcher_gnocchi.archive_policy,
                                    plugin_manager)
                for r in data.get('resources', [])]

    @property
    def gnocchi_project_id(self):
        if self._gnocchi_project_id is not None:
            return self._gnocchi_project_id
        with self._gnocchi_project_id_lock:
            if self._gnocchi_project_id is None:
                try:
                    project = self._ks_client.tenants.find(
                        name=self.conf.dispatcher_gnocchi.filter_project)
                except Exception:
                    LOG.exception('fail to retrieve user of Gnocchi service')
                    raise
                self._gnocchi_project_id = project.id
                LOG.debug("gnocchi project found: %s", self.gnocchi_project_id)
            return self._gnocchi_project_id

    def _is_swift_account_sample(self, sample):
        return bool([rd for rd in self.resources_definition
                     if rd.cfg['resource_type'] == 'swift_account'
                     and rd.match(sample['counter_name'])])

    def _is_gnocchi_activity(self, sample):
        return (self.filter_service_activity and (
            # avoid anything from the user used by gnocchi
            sample['project_id'] == self.gnocchi_project_id or
            # avoid anything in the swift account used by gnocchi
            (sample['resource_id'] == self.gnocchi_project_id and
             self._is_swift_account_sample(sample))
        ))

    def _get_resource_definition(self, metric_name):
        for rd in self.resources_definition:
            if rd.match(metric_name):
                return rd

    def record_metering_data(self, data):
        # NOTE(sileht): skip sample generated by gnocchi itself
        data = [s for s in data if not self._is_gnocchi_activity(s)]

        # FIXME(sileht): This method bulk the processing of samples
        # grouped by resource_id and metric_name but this is not
        # efficient yet because the data received here doesn't often
        # contains a lot of different kind of samples
        # So perhaps the next step will be to pool the received data from
        # message bus.
        data.sort(key=lambda s: (s['resource_id'], s['counter_name']))

        resource_grouped_samples = itertools.groupby(
            data, key=operator.itemgetter('resource_id'))

        for resource_id, samples_of_resource in resource_grouped_samples:
            metric_grouped_samples = itertools.groupby(
                list(samples_of_resource),
                key=operator.itemgetter('counter_name'))

            self._process_resource(resource_id, metric_grouped_samples)

    @log_and_ignore_unexpected_workflow_error
    def _process_resource(self, resource_id, metric_grouped_samples):
        resource_extra = {}
        for metric_name, samples in metric_grouped_samples:
            samples = list(samples)
            rd = self._get_resource_definition(metric_name)
            if rd is None:
                LOG.warn("metric %s is not handled by gnocchi" %
                         metric_name)
                continue
            if rd.cfg.get("ignore"):
                continue

            resource_type = rd.cfg['resource_type']
            resource = {
                "id": resource_id,
                "user_id": samples[0]['user_id'],
                "project_id": samples[0]['project_id'],
                "metrics": rd.metrics(),
            }
            measures = []

            for sample in samples:
                resource_extra.update(rd.attributes(sample))
                measures.append({'timestamp': sample['timestamp'],
                                 'value': sample['counter_volume']})

            resource.update(resource_extra)

            try:
                self._gnocchi.post_measure(resource_type, resource_id,
                                           metric_name, measures)
            except gnocchi_client.NoSuchMetric:
                # TODO(sileht): Make gnocchi smarter to be able to detect 404
                # for 'resource doesn't exist' and for 'metric doesn't exist'
                # https://bugs.launchpad.net/gnocchi/+bug/1476186
                self._ensure_resource_and_metric(resource_type, resource,
                                                 metric_name)

                try:
                    self._gnocchi.post_measure(resource_type, resource_id,
                                               metric_name, measures)
                except gnocchi_client.NoSuchMetric:
                    LOG.error(_LE("Fail to post measures for "
                                  "%(resource_id)s/%(metric_name)s") %
                              dict(resource_id=resource_id,
                                   metric_name=metric_name))

        if resource_extra:
            if self.cache:
                cache_key = cache_key_mangler(resource['id'])
                attribute_hash = self._check_resource_cache(
                    cache_key, resource)
                if attribute_hash:
                    with self._gnocchi_resource_lock:
                        attribute_hash = self._check_resource_cache(
                            cache_key, resource)
                        if attribute_hash:
                            self._gnocchi.update_resource(resource_type,
                                                          resource_id,
                                                          resource_extra)
                            self.cache.set(cache_key, attribute_hash, 3600)
                        else:
                            LOG.debug('recheck resource cache hit for '
                                      'update %s', resource['id'])
                else:
                    LOG.debug('resource cache hit for update %s',
                              resource['id'])
            else:
                self._gnocchi.update_resource(resource_type, resource_id,
                                              resource_extra)

    def _check_resource_cache(self, key, resource_data):
        resource_info = copy.deepcopy(resource_data)
        if 'metrics' in resource_info:
            del resource_info['metrics']
        attribute_hash = hash(frozenset(resource_info.items()))
        cached_hash = self.cache.get(key)
        if cached_hash != attribute_hash:
            return attribute_hash
        return None

    def _ensure_resource_and_metric(self, resource_type, resource,
                                    metric_name):
        try:
            if self.cache:
                cache_key = cache_key_mangler(resource['id'])
                attribute_hash = self._check_resource_cache(
                    cache_key, resource)
                if attribute_hash:
                    with self._gnocchi_resource_lock:
                        attribute_hash = self._check_resource_cache(
                            cache_key, resource)
                        if attribute_hash:
                            self._gnocchi.create_resource(resource_type,
                                                          resource)
                            self.cache.set(cache_key, attribute_hash, 3600)
                        else:
                            LOG.debug('recheck resource cache hit for '
                                      'create %s', resource['id'])
                else:
                    LOG.debug('recheck resource cache hit for create %s',
                              resource['id'])
            else:
                self._gnocchi.create_resource(resource_type, resource)
        except gnocchi_client.ResourceAlreadyExists:
            try:
                archive_policy = resource['metrics'][metric_name]
                self._gnocchi.create_metric(resource_type, resource['id'],
                                            metric_name, archive_policy)
            except gnocchi_client.MetricAlreadyExists:
                # NOTE(sileht): Just ignore the metric have been
                # created in the meantime.
                pass
        try:
            archive_policy = resource['metrics'][metric_name]
            self._gnocchi.create_metric(resource_type, resource['id'],
                                        metric_name, archive_policy)
        except gnocchi_client.MetricAlreadyExists:
            # NOTE(sileht): Just ignore the metric have been
            # created in the meantime.
            pass

    @staticmethod
    def record_events(events):
        pass
