#
# Copyright 2014 Red Hat, Inc
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

from oslo_config import cfg
from oslo_utils import timeutils

from ceilometer.agent import plugin_base
from ceilometer import nova_client

OPTS = [
    cfg.BoolOpt('workload_partitioning',
                default=False,
                help='Enable work-load partitioning, allowing multiple '
                     'compute agents to be run simultaneously.'),
    cfg.IntOpt('instance_query_period',
               default=120,
               help='period to query instance info through'
                    'nova client'),
]
cfg.CONF.register_opts(OPTS, group='compute')


class InstanceDiscovery(plugin_base.DiscoveryBase):
    def __init__(self):
        super(InstanceDiscovery, self).__init__()
        self.nova_cli = nova_client.Client()
        self.last_run = None
        self.instances = {}
        self.period = cfg.CONF.compute.instance_query_period

    def discover(self, manager, param=None):
        """Discover resources to monitor."""
        if not self.last_run:
            instances = self.nova_cli.instance_get_all_by_host(cfg.CONF.host)
            for instance in instances:
                if getattr(instance, 'OS-EXT-STS:vm_state', None) \
                        in ['deleted', 'error']:
                    self.instances.pop(instance.id, None)
                else:
                    self.instances[instance.id] = instance
            self.last_run = timeutils.utcnow()
        elif (timeutils.utcnow() - self.last_run).seconds > self.period:
            instances = self.nova_cli.\
                instance_get_all_by_host(cfg.CONF.host,
                                         self.last_run.isoformat())
            for instance in instances:
                if getattr(instance, 'OS-EXT-STS:vm_state', None) \
                        in ['deleted', 'error']:
                    self.instances.pop(instance.id, None)
                else:
                    self.instances[instance.id] = instance
            self.last_run = timeutils.utcnow()

        return self.instances.values()

    @property
    def group_id(self):
        if cfg.CONF.compute.workload_partitioning:
            return cfg.CONF.host
        else:
            return None
