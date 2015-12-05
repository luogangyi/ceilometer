#
# Copyright 2012-2013 eNovance <licensing@enovance.com>
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

from keystoneclient import exceptions as ks_exceptions
from oslo_config import cfg

from ceilometer.agent import base
from ceilometer import keystone_client
from ceilometer.openstack.common import log

OPTS = [
    cfg.StrOpt('partitioning_group_prefix',
               default=None,
               deprecated_group='central',
               help='Work-load partitioning group prefix. Use only if you '
                    'want to run multiple polling agents with different '
                    'config files. For each sub-group of the agent '
                    'pool with the same partitioning_group_prefix a disjoint '
                    'subset of pollsters should be loaded.'),
]

cfg.CONF.register_opts(OPTS, group='polling')

LOG = log.getLogger(__name__)


class AgentManager(base.AgentManager):

    def __init__(self, namespaces=None, pollster_list=None):
        namespaces = namespaces or ['compute', 'central']
        pollster_list = pollster_list or []
        self._keystone = None
        self._keystone_last_exception = None
        super(AgentManager, self).__init__(
            namespaces, pollster_list,
            group_prefix=cfg.CONF.polling.partitioning_group_prefix)

    def interval_task(self, task):
        # NOTE(sileht): remove the previous keystone client
        # and exception to get a new one in this polling cycle.
        self._keystone = None
        self._keystone_last_exception = None

        super(AgentManager, self).interval_task(task)

    @property
    def keystone(self):
        # NOTE(sileht): we do lazy loading of the keystone client
        # for multiple reasons:
        # * don't use it if no plugin need it
        # * use only one client for all plugins per polling cycle
        if self._keystone is None and self._keystone_last_exception is None:
            try:
                self._keystone = keystone_client.get_client()
                self._keystone_last_exception = None
            except ks_exceptions.ClientException as e:
                self._keystone = None
                self._keystone_last_exception = e
        if self._keystone is not None:
            return self._keystone
        else:
            raise self._keystone_last_exception