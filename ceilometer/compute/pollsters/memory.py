# Copyright (c) 2014 VMware, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import ceilometer
from ceilometer.compute import pollsters
from ceilometer.compute.pollsters import util
from ceilometer.compute.virt import inspector as virt_inspector
from ceilometer.i18n import _, _LW, _LE
from ceilometer.openstack.common import log
from ceilometer import sample

LOG = log.getLogger(__name__)


class MemoryUsagePollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        self._inspection_duration = self._record_poll_time()
        for instance in resources:
            LOG.debug(_('Checking memory usage for instance %s'), instance.id)
            try:
                memory_info = self.inspector.inspect_memory_usage(
                    instance, self._inspection_duration)
                LOG.debug(_("MEMORY USAGE: %(instance)s %(usage)f"),
                          ({'instance': instance.__dict__,
                            'usage': memory_info.usage}))
                yield util.make_sample_from_instance(
                    instance,
                    name='memory.usage',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=memory_info.usage,
                )
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except virt_inspector.InstanceShutOffException as e:
                LOG.warn(_LW('Instance %(instance_id)s was shut off while '
                             'getting samples of %(pollster)s: %(exc)s'),
                         {'instance_id': instance.id,
                          'pollster': self.__class__.__name__, 'exc': e})
            except virt_inspector.NoDataException as e:
                LOG.warn(_LW('Cannot inspect data of %(pollster)s for '
                             '%(instance_id)s, non-fatal reason: %(exc)s'),
                         {'pollster': self.__class__.__name__,
                          'instance_id': instance.id, 'exc': e})
            except ceilometer.NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Obtaining Memory Usage is not implemented for %s'
                            ), self.inspector.__class__.__name__)
            except Exception as err:
                LOG.exception(_('Could not get Memory Usage for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class MemoryResidentPollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        self._inspection_duration = self._record_poll_time()
        for instance in resources:
            LOG.debug(_('Checking resident memory for instance %s'),
                      instance.id)
            try:
                memory_info = self.inspector.inspect_memory_resident(
                    instance, self._inspection_duration)
                LOG.debug(_("RESIDENT MEMORY: %(instance)s %(resident)f"),
                          ({'instance': instance.__dict__,
                            'resident': memory_info.resident}))
                yield util.make_sample_from_instance(
                    instance,
                    name='memory.resident',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=memory_info.resident,
                )
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except virt_inspector.InstanceShutOffException as e:
                LOG.warn(_LW('Instance %(instance_id)s was shut off while '
                             'getting samples of %(pollster)s: %(exc)s'),
                         {'instance_id': instance.id,
                          'pollster': self.__class__.__name__, 'exc': e})
            except virt_inspector.NoDataException as e:
                LOG.warn(_LW('Cannot inspect data of %(pollster)s for '
                             '%(instance_id)s, non-fatal reason: %(exc)s'),
                         {'pollster': self.__class__.__name__,
                          'instance_id': instance.id, 'exc': e})
            except ceilometer.NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Obtaining Resident Memory is not implemented'
                            ' for %s'), self.inspector.__class__.__name__)
            except Exception as err:
                LOG.exception(_LE('Could not get Resident Memory Usage for '
                                  '%(id)s: %(e)s'), {'id': instance.id,
                                                     'e': err})


class MemoryTotalPollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking memory total for instance %s'),
                      instance.id)
            try:
                mem_info = self.inspector.inspect_memory_info(instance)
                if mem_info is None:
                    raise NotImplementedError
                memory_total = int(mem_info['total'])
                LOG.debug(_("MEMORY TOTAL: %(instance)s %(total)d"),
                          ({'instance': instance.__dict__,
                            'total': memory_total}))

                yield util.make_sample_from_instance(
                    instance,
                    name='memory.total',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=memory_total,
                )
            except virt_inspector.InstanceNoQGAException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('QEMU-GUEST-AGENT is not installed or'
                            ' started in %s'), instance.id)
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Cannot get Memory Total for instance %s, '
                            'maybe it is not implemented in qemu-guest-agent'
                            ), instance.id)
            except Exception as err:
                LOG.exception(_('Could not get Memory Total for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class MemoryUnusedPollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking memory unused for instance %s'),
                      instance.id)
            try:
                mem_info = self.inspector.inspect_memory_info(instance)
                if mem_info is None:
                    raise NotImplementedError
                memory_unused = int(mem_info['total']) - int(mem_info['used'])
                LOG.debug(_("MEMORY UNUSED: %(instance)s %(unused)d"),
                          ({'instance': instance.__dict__,
                            'unused': memory_unused}))

                yield util.make_sample_from_instance(
                    instance,
                    name='memory.unused',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=memory_unused,
                )
            except virt_inspector.InstanceNoQGAException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('QEMU-GUEST-AGENT is not installed or'
                            ' started in %s'), instance.id)
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Cannot get Memory Unused for instance %s, '
                            'maybe it is not implemented in qemu-guest-agent'
                            ), instance.id)
            except Exception as err:
                LOG.exception(_('Could not get Memory Unsed for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class MemorySwapTotalPollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking memory swap total for instance %s'),
                      instance.id)
            try:
                mem_info = self.inspector.inspect_memory_info(instance)
                if mem_info is None:
                    raise NotImplementedError
                swap_total = int(mem_info['swap_total'])
                LOG.debug(_("MEMORY SWAP TOTAL: %(instance)s %(swap_total)d"),
                          ({'instance': instance.__dict__,
                            'swap_total': swap_total}))

                yield util.make_sample_from_instance(
                    instance,
                    name='memory.swap.total',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=swap_total,
                )
            except virt_inspector.InstanceNoQGAException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('QEMU-GUEST-AGENT is not installed or'
                            ' started in %s'), instance.id)
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Cannot get Memory Swap Total for instance %s, '
                            'maybe it is not implemented in qemu-guest-agent'
                            ), instance.id)
            except Exception as err:
                LOG.exception(_('Could not get Memory Swap Total for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class MemorySwapFreePollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking memory swap free for instance %s'),
                      instance.id)
            try:
                mem_info = self.inspector.inspect_memory_info(instance)
                if mem_info is None:
                    raise NotImplementedError
                swap_free = int(mem_info['swap_total']) - \
                            int(mem_info['swap_used'])
                LOG.debug(_("MEMORY SWAP FREE: %(instance)s %(swap_free)d"),
                          ({'instance': instance.__dict__,
                            'swap_free': swap_free}))

                yield util.make_sample_from_instance(
                    instance,
                    name='memory.swap.free',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=swap_free,
                )
            except virt_inspector.InstanceNoQGAException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('QEMU-GUEST-AGENT is not installed or'
                            ' started in %s'), instance.id)
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Cannot get Memory Swap Free for instance %s, '
                            'maybe it is not implemented in qemu-guest-agent'
                            ), instance.id)
            except Exception as err:
                LOG.exception(_('Could not get Memory Swap Free for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class MemoryBufferPollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking memory buffer for instance %s'),
                      instance.id)
            try:
                mem_info = self.inspector.inspect_memory_info(instance)
                if mem_info is None:
                    raise NotImplementedError
                memory_buffer = int(mem_info['buffer'])
                LOG.debug(_("MEMORY BUFFER: %(instance)s %(memory_buffer)d"),
                          ({'instance': instance.__dict__,
                            'memory_buffer': memory_buffer}))

                yield util.make_sample_from_instance(
                    instance,
                    name='memory.buffer',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=memory_buffer,
                )
            except virt_inspector.InstanceNoQGAException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('QEMU-GUEST-AGENT is not installed or'
                            ' started in %s'), instance.id)
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Cannot get Memory Buffer for instance %s, '
                            'maybe it is not implemented in qemu-guest-agent'
                            ), instance.id)
            except Exception as err:
                LOG.exception(_('Could not get Memory Buffer for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class MemoryCachedPollster(pollsters.BaseComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking memory cached for instance %s'),
                      instance.id)
            try:
                mem_info = self.inspector.inspect_memory_info(instance)
                if mem_info is None:
                    raise NotImplementedError
                memory_cached = int(mem_info['cached'])
                LOG.debug(_("MEMORY CACHED: %(instance)s %(memory_cached)d"),
                          ({'instance': instance.__dict__,
                            'memory_cached': memory_cached}))

                yield util.make_sample_from_instance(
                    instance,
                    name='memory.cached',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=memory_cached,
                )
            except virt_inspector.InstanceNoQGAException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('QEMU-GUEST-AGENT is not installed or'
                            ' started in %s'), instance.id)
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Cannot get Memory Cached for instance %s, '
                            'maybe it is not implemented in qemu-guest-agent'
                            ), instance.id)
            except Exception as err:
                LOG.exception(_('Could not get Memory Cached for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})
