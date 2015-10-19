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

try:
    import queue
except ImportError:
    import Queue as queue

import six

import bucky.udpserver as udpserver
from bucky.errors import ConfigError, ProtocolError

# log = multiprocessing.log_to_stderr(logging.DEBUG)
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
    def __init__(self, outbox, config):
        super(StatsDHandler, self).__init__()
        self.daemon = True
        self.outbox = outbox
        self.inbox = queue.Queue()
        self.cfg = config
        self.lock = threading.Lock()
        self.timers = {}
        self.gauges = {}
        self.counters = {}
        self.sets = {}
        self.flush_time = config.statsd_flush_time
        self.legacy_namespace = config.statsd_legacy_namespace
        self.global_prefix = config.statsd_global_prefix
        self.prefix_counter = config.statsd_prefix_counter
        self.prefix_timer = config.statsd_prefix_timer
        self.prefix_gauge = config.statsd_prefix_gauge
        self.prefix_set = config.statsd_prefix_set
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

        self.statsd_persistent_gauges = config.statsd_persistent_gauges
        self.gauges_filename = os.path.join(self.cfg.directory, self.cfg.statsd_gauges_savefile)

        self.keys_seen = set()
        self.delete_idle_stats = config.statsd_delete_idlestats
        self.delete_counters = self.delete_idle_stats and config.statsd_delete_counters
        self.delete_timers = self.delete_idle_stats and config.statsd_delete_timers
        self.delete_sets = self.delete_idle_stats and config.statsd_delete_sets
        self.only_changed_gauges = self.delete_idle_stats and config.statsd_onlychanged_gauges
        self.next_update = time.time() + self.flush_time

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

    def _flush(self):
        name_global_num_stats = self.name_global + "numStats"
        stat_time = int(time.time())
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
            num_stats = self.enqueue_timers(stat_time)
            num_stats += self.enqueue_counters(stat_time)
            num_stats += self.enqueue_gauges(stat_time)
            num_stats += self.enqueue_sets(stat_time)
            self.enqueue(name_global_num_stats, num_stats, stat_time)
            self.keys_seen = set()
        return num_stats

    @staticmethod
    def debug_msg(message, loop_count, *args):
        msg = '[%f] %s - %d ' + message
        log.debug(msg, time.time(), threading.current_thread().ident, loop_count, *args)

    def run(self):
        self.name = "bucky: %s" % self.__class__.__name__
        loop_count = 0
        while True:
            loop_count += 1
            to_sleep = self.next_update - time.time()
            self.debug_msg('Loop Start to_sleep : %f\tnext_update: %f\tflush_time: %f', loop_count, to_sleep,
                           self.next_update, self.flush_time)
            if to_sleep <= 0:
                self.debug_msg('Flushing stats', loop_count)
                self._flush()
                self.next_update = time.time() + self.flush_time
                to_sleep = self.flush_time
                self.debug_msg('Post Flush to_sleep : %f\tnext_update: %f\tflush_time: %f', loop_count, to_sleep,
                               self.next_update, self.flush_time)
            try:
                self.debug_msg('Pre Queue to_sleep : %f\tnext_update: %f\tflush_time: %f', loop_count, to_sleep,
                               self.next_update, self.flush_time)
                metric = self.inbox.get(True, to_sleep)
                if metric is None:
                    log.info("Handler received None, %s exiting", self)
                    break
                self.debug_msg('Handling metric %r ', loop_count, metric)
                self.handle(metric)
            except queue.Empty:
                self.debug_msg('Empty Queue', loop_count)
                continue

    def enqueue(self, name, stat, stat_time):
        # No hostnames on statsd
        self.outbox.put((None, name, stat, stat_time))

    def enqueue_timers(self, stat_time):
        ret = 0
        iteritems = self.timers.items() if six.PY3 else self.timers.iteritems()
        for k, v in iteritems:
            # Skip timers that haven't collected any values
            if not v:
                self.enqueue("%s%s.count" % (self.name_timer, k), 0, stat_time)
                self.enqueue("%s%s.count_ps" % (self.name_timer, k), 0.0, stat_time)
            else:
                v.sort()
                pct_thresh = 90
                count = len(v)
                value_min, value_max = v[0], v[-1]
                mean, value_thresh = value_min, value_max

                if count > 1:
                    thresh_idx = int(math.floor(pct_thresh / 100.0 * count))
                    v = v[:thresh_idx]
                    value_thresh = v[-1]
                    value_sum = sum(v)
                    mean = value_sum / float(len(v))

                self.enqueue("%s%s.mean" % (self.name_timer, k), mean, stat_time)
                self.enqueue("%s%s.upper" % (self.name_timer, k), value_max, stat_time)
                t = int(pct_thresh)
                self.enqueue("%s%s.upper_%s" % (self.name_timer, k, t), value_thresh, stat_time)
                self.enqueue("%s%s.lower" % (self.name_timer, k), value_min, stat_time)
                self.enqueue("%s%s.count" % (self.name_timer, k), count, stat_time)
                self.enqueue("%s%s.count_ps" % (self.name_timer, k), float(count) / self.flush_time, stat_time)
            self.timers[k] = []
            ret += 1

        return ret

    def enqueue_sets(self, stat_time):
        ret = 0
        iteritems = self.sets.items() if six.PY3 else self.sets.iteritems()
        for k, v in iteritems:
            self.enqueue("%s%s.count" % (self.name_set, k), len(v), stat_time)
            ret += 1
            self.sets[k] = set()
        return ret

    def enqueue_gauges(self, stat_time):
        ret = 0
        iteritems = self.gauges.items() if six.PY3 else self.gauges.iteritems()
        for k, v in iteritems:
            # only send a value if there was an update if `delete_idlestats` is `True`
            if not self.only_changed_gauges or k in self.keys_seen:
                self.enqueue("%s%s" % (self.name_gauge, k), v, stat_time)
                ret += 1
        return ret

    def enqueue_counters(self, stat_time):
        ret = 0
        iteritems = self.counters.items() if six.PY3 else self.counters.iteritems()
        for k, v in iteritems:
            if self.legacy_namespace:
                stat_rate = "%s%s" % (self.name_legacy_rate, k)
                stat_count = "%s%s" % (self.name_legacy_count, k)
            else:
                stat_rate = "%s%s.rate" % (self.name_counter, k)
                stat_count = "%s%s.count" % (self.name_counter, k)
            self.enqueue(stat_rate, v / self.flush_time, stat_time)
            self.enqueue(stat_count, v, stat_time)
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

    def put(self, metric):
        log.debug('Handler Received %r', metric)
        self.inbox.put(metric)

    def handle_timer(self, metric):
        try:
            with self.lock:
                self.timers.setdefault(metric.name, []).append(float(metric.value or 0))
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
            value_string = str(metric.value or "0")
            with self.lock:
                if metric.name not in self.sets:
                    self.sets[metric.name] = set()
                self.sets[metric.name].add(value_string)
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

    @staticmethod
    def parse(data):
        log.debug('Parsing : %r', data)
        if six.PY3:
            data = data.decode()
        for line in data.splitlines():
            name, data = line.split(':', 1)
            for sample in data.split(':'):
                args = sample.split('|')
                metric = StatsDMetric(name, *args)
                log.debug('Received : %r', metric)
                yield metric


class StatsDServer(udpserver.UDPServer):
    def __init__(self, inbox, config):
        super(StatsDServer, self).__init__(config.statsd_ip, config.statsd_port)
        self.parser = StatsDParser()
        self.config = config
        self.inbox = inbox
        self.handlers = self._init_handlers(self.inbox, self.config)

    def pre_shutdown(self):
        for _, handler in self.handlers:
            handler.save_gauges()

    def run(self):
        log.debug('Starting Run Method')
        for _, handler in self.handlers:
            handler.start()
        super(StatsDServer, self).run()

    def handle(self, data, addr):
        log.debug('Received %r from %r', data, addr)
        try:
            for mc in self.parser.parse(data):
                handler = self._get_handler(mc.name)
                log.debug('Handling %r with handler %r', mc, handler)
                handler.put(mc)
        except ProtocolError:
            log.exception("Error from: %s:%s" % addr)
        return True

    @staticmethod
    def _init_handlers(inbox, config):
        ret = {}
        handlers = config.metricsd_handlers
        if not len(handlers):
            ret = [(None, StatsDHandler(inbox, config))]
            return ret
        handlers.sort(key=lambda p: p[2])
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
            patterns = ret.get(interval, [])
            patterns.append(pattern)
            ret[interval] = patterns
        # ret.append((pattern, interval, priority))
        ret = [(p, StatsDHandler(inbox, i)) for (i, p) in ret.items()]
        ret.append((None, StatsDHandler(inbox, config)))
        return ret

    def _get_handler(self, name):
        for (patterns, h) in self.handlers:
            if patterns is None:
                return h
            for pattern in patterns:
                if pattern.match(name):
                    return h

    def close(self):
        super(StatsDServer, self).close()
