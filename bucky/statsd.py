# -*- coding: utf-8 -
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# Copyright 2011 Cloudant, Inc.

import os
import re
import math
import time
import json
import logging
import threading

import six

import bucky.udpserver as udpserver
from bucky.errors import ConfigError, ProtocolError
import bucky.cfg as cfg


log = logging.getLogger(__name__)

try:
    from io import open
except ImportError:
    # Python <2.6
    _open = open

    def open(*args, **kwargs):
        """
        Wrapper around open which does not support 'encoding' keyword in
        older versions of Python
        """
        kwargs.pop("encoding")
        return _open(*args, **kwargs)

if six.PY3:
    def read_json_file(gauges_filename):
        with open(gauges_filename, mode='r', encoding='utf-8') as f:
            return json.load(f)

    def write_json_file(gauges_filename, gauges):
        with open(gauges_filename, mode='w', encoding='utf-8') as f:
            json.dump(gauges, f)
else:
    def read_json_file(gauges_filename):
        with open(gauges_filename, mode='rb') as f:
            return json.load(f)

    def write_json_file(gauges_filename, gauges):
        with open(gauges_filename, mode='wb') as f:
            json.dump(gauges, f)


def make_name(parts):
    name = ""
    for part in parts:
        if part:
            name = name + part + "."
    return name


class StatsDHandler(threading.Thread):
    def __init__(self, queue, cfg):
        super(StatsDHandler, self).__init__()
        self.daemon = True
        self.queue = queue
        self.cfg = cfg
        self.lock = threading.Lock()
        self.timers = {}
        self.gauges = {}
        self.counters = {}
        self.sets = {}
        self.flush_time = cfg.statsd_flush_time
        self.legacy_namespace = cfg.statsd_legacy_namespace
        self.global_prefix = cfg.statsd_global_prefix
        self.prefix_counter = cfg.statsd_prefix_counter
        self.prefix_timer = cfg.statsd_prefix_timer
        self.prefix_gauge = cfg.statsd_prefix_gauge
        self.prefix_set = cfg.statsd_prefix_set
        self.key_res = (
            (re.compile("\s+"), "_"),
            (re.compile("\/"), "-"),
            (re.compile("[^a-zA-Z_\-0-9\.]"), "")
        )

        if self.legacy_namespace:
            self.name_global = 'stats.'
            self.name_legacy_rate = 'stats.'
            self.name_legacy_count = 'stats_counts.'
            self.name_timer = 'stats.timers.'
            self.name_gauge = 'stats.gauges.'
            self.name_set = 'stats.sets.'
        else:
            self.name_global = make_name([self.global_prefix])
            self.name_counter = make_name([self.global_prefix, self.prefix_counter])
            self.name_timer = make_name([self.global_prefix, self.prefix_timer])
            self.name_gauge = make_name([self.global_prefix, self.prefix_gauge])
            self.name_set = make_name([self.global_prefix, self.prefix_set])

        self.statsd_persistent_gauges = cfg.statsd_persistent_gauges
        self.gauges_filename = os.path.join(self.cfg.directory, self.cfg.statsd_gauges_savefile)

        self.keys_seen = set()
        self.delete_idlestats = cfg.statsd_delete_idlestats
        self.delete_counters = self.delete_idlestats and cfg.statsd_delete_counters
        self.delete_timers = self.delete_idlestats and cfg.statsd_delete_timers
        self.delete_sets = self.delete_idlestats and cfg.statsd_delete_sets
        self.onlychanged_gauges = self.delete_idlestats and cfg.statsd_onlychanged_gauges

    def load_gauges(self):
        if not self.statsd_persistent_gauges:
            return
        if not os.path.isfile(self.gauges_filename):
            return
        log.info("StatsD: Loading saved gauges %s", self.gauges_filename)
        try:
            gauges = read_json_file(self.gauges_filename)
        except IOError:
            log.exception("StatsD: IOError")
        else:
            self.gauges.update(gauges)
            self.keys_seen.update(gauges.keys())

    def save_gauges(self):
        if not self.statsd_persistent_gauges:
            return
        try:
            write_json_file(self.gauges_filename, self.gauges)
        except IOError:
            log.exception("StatsD: IOError")

    def run(self):
        name_global_numstats = self.name_global + "numStats"
        while True:
            time.sleep(self.flush_time)
            stime = int(time.time())
            with self.lock:
                if self.delete_timers:
                    rem_keys = set(self.timers.keys()) - self.keys_seen
                    for k in rem_keys:
                        del self.timers[k]
                if self.delete_counters:
                    rem_keys = set(self.counters.keys()) - self.keys_seen
                    for k in rem_keys:
                        del self.counters[k]
                if self.delete_sets:
                    rem_keys = set(self.sets.keys()) - self.keys_seen
                    for k in rem_keys:
                        del self.sets[k]
                num_stats = self.enqueue_timers(stime)
                num_stats += self.enqueue_counters(stime)
                num_stats += self.enqueue_gauges(stime)
                num_stats += self.enqueue_sets(stime)
                self.enqueue(name_global_numstats, num_stats, stime)
                self.keys_seen = set()

    def enqueue(self, name, stat, stime):
        # No hostnames on statsd
        self.queue.put((None, name, stat, stime))

    def enqueue_timers(self, stime):
        ret = 0
        iteritems = self.timers.items() if six.PY3 else self.timers.iteritems()
        for k, v in iteritems:
            # Skip timers that haven't collected any values
            if not v:
                self.enqueue("%s%s.count" % (self.name_timer, k), 0, stime)
                self.enqueue("%s%s.count_ps" % (self.name_timer, k), 0.0, stime)
            else:
                v.sort()
                pct_thresh = 90
                count = len(v)
                vmin, vmax = v[0], v[-1]
                mean, vthresh = vmin, vmax

                if count > 1:
                    thresh_idx = int(math.floor(pct_thresh / 100.0 * count))
                    v = v[:thresh_idx]
                    vthresh = v[-1]
                    vsum = sum(v)
                    mean = vsum / float(len(v))

                self.enqueue("%s%s.mean" % (self.name_timer, k), mean, stime)
                self.enqueue("%s%s.upper" % (self.name_timer, k), vmax, stime)
                t = int(pct_thresh)
                self.enqueue("%s%s.upper_%s" % (self.name_timer, k, t), vthresh, stime)
                self.enqueue("%s%s.lower" % (self.name_timer, k), vmin, stime)
                self.enqueue("%s%s.count" % (self.name_timer, k), count, stime)
                self.enqueue("%s%s.count_ps" % (self.name_timer, k), float(count) / self.flush_time, stime)
            self.timers[k] = []
            ret += 1

        return ret

    def enqueue_sets(self, stime):
        ret = 0
        iteritems = self.sets.items() if six.PY3 else self.sets.iteritems()
        for k, v in iteritems:
            self.enqueue("%s%s.count" % (self.name_set, k), len(v), stime)
            ret += 1
            self.sets[k] = set()
        return ret

    def enqueue_gauges(self, stime):
        ret = 0
        iteritems = self.gauges.items() if six.PY3 else self.gauges.iteritems()
        for k, v in iteritems:
            # only send a value if there was an update if `delete_idlestats` is `True`
            if not self.onlychanged_gauges or k in self.keys_seen:
                self.enqueue("%s%s" % (self.name_gauge, k), v, stime)
                ret += 1
        return ret

    def enqueue_counters(self, stime):
        ret = 0
        iteritems = self.counters.items() if six.PY3 else self.counters.iteritems()
        for k, v in iteritems:
            if self.legacy_namespace:
                stat_rate = "%s%s" % (self.name_legacy_rate, k)
                stat_count = "%s%s" % (self.name_legacy_count, k)
            else:
                stat_rate = "%s%s.rate" % (self.name_counter, k)
                stat_count = "%s%s.count" % (self.name_counter, k)
            self.enqueue(stat_rate, v / self.flush_time, stime)
            self.enqueue(stat_count, v, stime)
            self.counters[k] = 0
            ret += 1
        return ret

    def handle(self, metric):
        if metric.type == StatsDMetric.TIMER:
            self.handle_timer(metric)
        elif metric.type == StatsDMetric.GAUGE:
            self.handle_gauge(metric)
        elif metric.type == StatsDMetric.SET:
            self.handle_set(metric)
        elif metric.type == StatsDMetric.COUNTER:
            self.handle_counter(metric)
        else:
            log.error('Unhandled metric type "%s"', metric.type)

    def handle_timer(self, metric):
        try:
            val = float(metric.value or 0)
            with self.lock:
                self.timers.setdefault(metric.name, []).append(metric.value)
        except:
            self.bad_metric(metric)

    def handle_gauge(self, metric):
        try:
            val = float(metric.value or 0)
            with self.lock:
                if metric.name in self.gauges and metric.sign is not None:
                    self.gauges[metric.name] = self.gauges[metric.name] + metric.name
                else:
                    self.gauges[metric.name] = val
        except:
            self.bad_metric(metric)

    def handle_set(self, metric):
        try:
            valstr = str(metric.value or "0")
            with self.lock:
                if metric.name not in self.sets:
                    self.sets[metric.name] = set()
                self.sets[metric.name].add(valstr)
        except:
            self.bad_metric(metric)

    def handle_counter(self, metric):
        try:
            with self.lock:
                if metric.name not in self.counters:
                    self.counters[metric.name] = 0
                self.counters[metric.name] += int(float(metric.value or 0) / metric.rate)
        except:
            self.bad_metric(metric)

    def bad_metric(self, metric, msg=None):
        log.error("StatsD: Invalid metric: '%s'", metric)
        if msg:
            log.error(msg)

    def close(self):
        pass


class StatsDMetric(object):
    GAUGE = 'g'
    COUNTER = 'c'
    TIMER = 'ms'
    HISTOGRAM = 'h'
    METER = 'm'
    SET = 's'

    def __init__(self, name, value=1.0, metric_type=METER, rate=None):
        self.name = name
        if value[0] in ['-', '+']:
            self.sign = value[0]
        else:
            self.sign = None
        self.value = float(value)
        assert (metric_type in [self.GAUGE, self.COUNTER, self.TIMER, self.HISTOGRAM, self.METER])
        self.type = metric_type
        if rate is not None:
            try:
                if rate[0] == '@':
                    rate = rate[1:]
            except TypeError:
                pass
            self.rate = float(rate)
        else:
            self.rate = 1.0


class StatsDParser(object):
    def __init__(self):
        pass

    def parse(self, data):
        if six.PY3:
            data = data.decode()
        for line in data.splitlines():
            name, data = line.split(':', 1)
            for sample in data.split(':'):
                args = sample.split('|')
                metric = StatsDMetric(name, *args)
                yield metric


class StatsDServer(udpserver.UDPServer):
    def __init__(self, queue, cfg):
        super(StatsDServer, self).__init__(cfg.statsd_ip, cfg.statsd_port)
        self.parser = StatsDParser()
        self.handlers = self._init_handlers(queue, cfg)
        log.debug('Handlers: %r', self.handlers)

    def pre_shutdown(self):
        for _, handler in self.handlers:
            handler.save_gauges()

    def run(self):
        log.debug('Starting Run Method')
        super(StatsDServer, self).run()

    def handle(self, data, addr):
        try:
            for mc in self.parser.parse(data):
                handler = self._get_handler(mc.name)
                handler.handle(mc)
        except ProtocolError:
            log.exception("Error from: %s:%s" % addr)

    def _init_handlers(self, queue, cfg):
        ret = []
        handlers = cfg.metricsd_handlers
        if not len(handlers):
            ret = [(None, StatsDHandler(queue, cfg))]
            ret[0][1].start()
            return ret
        for item in handlers:
            if len(item) == 2:
                pattern, interval, priority = item[0], item[1], 100
            elif len(item) == 3:
                pattern, interval, priority = item
            else:
                raise ConfigError("Invalid handler specification: %s" % item)
            try:
                pattern = re.compile(pattern)
            except:
                raise ConfigError("Invalid pattern: %s" % pattern)
            if interval < 0:
                raise ConfigError("Invalid interval: %s" % interval)
            ret.append((pattern, interval, priority))
        handlers.sort(key=lambda p: p[2])
        ret = [(p, StatsDHandler(queue, i)) for (p, i, _) in ret]
        ret.append((None, StatsDHandler(queue, cfg)))
        for _, h in ret:
            h.start()
        return ret

    def _get_handler(self, name):
        for (p, h) in self.handlers:
            if p is None:
                return h
            if p.match(name):
                return h

    def close(self):
        for _, handler in self.handlers:
            handler.close()
            handler.join(cfg.process_join_timeout)
        super(StatsDServer, self).close()
