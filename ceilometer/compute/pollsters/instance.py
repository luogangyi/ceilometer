#
# Copyright 2012 eNovance <licensing@enovance.com>
# Copyright 2012 Red Hat, Inc
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

from ceilometer.compute import pollsters
from ceilometer.compute.pollsters import util
from ceilometer.compute.virt import inspector as virt_inspector
from ceilometer.i18n import _, _LW, _LE
from ceilometer.openstack.common import log
from ceilometer import sample

LOG = log.getLogger(__name__)


class InstancePollster(pollsters.BaseComputePollster):

    @staticmethod
    def get_samples(manager, cache, resources):
        for instance in resources:
            yield util.make_sample_from_instance(
                instance,
                name='instance',
                type=sample.TYPE_GAUGE,
                unit='instance',
                volume=1,
            )


class InstanceFlavorPollster(pollsters.BaseComputePollster):

    @staticmethod
    def get_samples(manager, cache, resources):
        for instance in resources:
            yield util.make_sample_from_instance(
                instance,
                # Use the "meter name + variable" syntax
                name='instance:%s' %
                instance.flavor['name'],
                type=sample.TYPE_GAUGE,
                unit='instance',
                volume=1,
            )


class InstanceSystemInfoPollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking system info for instance %s'),
                      instance.id)
            try:
                sys_info = self.inspector.inspect_system_info(instance)
                if sys_info is None:
                    raise NotImplementedError
                sys_info = str(sys_info)
                sys_meta = {'system_info': sys_info}
                LOG.debug(_("SYSTEM INFO: %(instance)s %(sys_info)s"),
                          ({'instance': instance.__dict__,
                            'sys_info': sys_info}))

                yield util.make_sample_from_instance(
                    instance,
                    name='instance.system.info',
                    type=sample.TYPE_GAUGE,
                    unit='instance',
                    volume=1,
                    additional_metadata=sys_meta,
                )
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Cannot get System Info for instance %s, '
                            'maybe it is not implemented in qemu-guest-agent'
                            ), instance.id)
            except Exception as err:
                LOG.exception(_('Could not get System info for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class InstanceOOMStatusPollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking oom status for instance %s'),
                      instance.id)
            try:
                oom_info = self.inspector.inspect_oom_status(instance)
                if oom_info is None:
                    raise NotImplementedError
                if oom_info is True:
                    oom_status = 1
                else:
                    oom_status = 0

                LOG.debug(_("OOM STATUS: %(instance)s %(oom_status)d"),
                          ({'instance': instance.__dict__,
                            'oom_status': oom_status}))

                yield util.make_sample_from_instance(
                    instance,
                    name='instance.oom.status',
                    type=sample.TYPE_GAUGE,
                    unit='instance',
                    volume=oom_status,
                )
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Cannot get OOM Status for instance %s, '
                            'maybe it is not implemented in qemu-guest-agent'
                            ), instance.id)
            except Exception as err:
                LOG.exception(_('Could not get OOM Status for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class InstanceAppStatsPollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking app stats for instance %s'),
                      instance.id)
            try:
                app_info = self.inspector.inspect_app_stats(instance)
                if app_info is None:
                    raise NotImplementedError
                app_stats = str(app_info)
                app_meta = {'app_stats': app_stats}

                LOG.debug(_("APP STATS: %(instance)s %(app_stats)s"),
                          ({'instance': instance.__dict__,
                            'app_stats': app_stats}))

                yield util.make_sample_from_instance(
                    instance,
                    name='instance.app.stats',
                    type=sample.TYPE_GAUGE,
                    unit='instance',
                    volume=1,
                    additional_metadata=app_meta,
                )
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Cannot get APP Stats for instance %s, '
                            'maybe it is not implemented in qemu-guest-agent'
                            ), instance.id)
            except Exception as err:
                LOG.exception(_('Could not get APP Stats for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class InstancePingDelayPollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking ping delay for instance %s'),
                      instance.id)
            try:
                delay = self.inspector.inspect_ping_delay(instance)
                if delay is None:
                    raise NotImplementedError
                if delay == '':
                    # set timeout = 999
                    delay = 999
                else:
                    try:
                        delay = float(delay)
                    except:
                        delay = 999

                LOG.debug(_("PING DELAY: %(instance)s %(delay)f"),
                          ({'instance': instance.__dict__,
                            'delay': delay}))

                yield util.make_sample_from_instance(
                    instance,
                    name='instance.ping.delay',
                    type=sample.TYPE_GAUGE,
                    unit='instance',
                    volume=delay,
                )
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Cannot get Ping Delay for instance %s, '
                            'maybe it is not implemented in qemu-guest-agent'
                            ), instance.id)
            except Exception as err:
                LOG.exception(_('Could not get Ping Delay for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class InstanceUserCheckPollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        # to be implements
        yield