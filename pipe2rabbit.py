#!/usr/bin/env python3

from gc import collect as garbagecollect
from os.path import basename
from queue import Queue
from threading import Thread
import fileinput
import re
import sys

from pkg_resources import require as pkgrequire
pkgrequire("pika==0.13.*")
import pika

import yaml

# Requirements NOT in Python's standard library:
# Pika-0.13.* (pika-0.13.*)
# PyYAML (yaml)

class RMQ_Publisher(object):

    PUBLISH_INTERVAL = 1

    def __init__(self, s, queued_messages):
        self._stopping = False

        # BUG in Pika?: Re-connecting somehow works even though erase_on_connect=True. Investigate.
        self._credentials = (
            pika.PlainCredentials(s['username'],
                                  s['password'],
                                  erase_on_connect=True
                                  )
        )
        self._connect_parameters = (
            pika.ConnectionParameters(host=s['host'],
                                      port=s['port'],
                                      credentials=self._credentials,
                                      ssl=True
                                      )
        )
        self._connection = None
        self._channel = None

        self._exchange_name = s['exchange_name']
        self._exchange_type = s['exchange_type']
        self._queue = s['queue']
        self._routing_key = s['routing_key']

        self._messages = queued_messages
        self._msg = None

        # SECURITY: Clear as much credential information from memory as possible.
        del self._credentials
        s.clear()
        garbagecollect()

    def connect(self):
        print("Connecting to RabbitMQ")
        return pika.SelectConnection(
                        self._connect_parameters,
                        on_open_callback=self.on_connection_open,
                        on_open_error_callback=self.on_connection_open_error,
                        on_close_callback=self.on_connection_closed,
                        stop_ioloop_on_close=False
                        )

    def on_connection_open(self, _unused_connection):
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, _unused_error):
# This seems to work now, no need for self.run. Leave for when porting to pika-1.0.0 though.
        self._connection.ioloop.add_timeout(5, self._connection.ioloop.stop)
#        self._connection.ioloop.add_timeout(5, self.run)

    def on_connection_closed(self, connection, reply_code, reply_text):
        print("Inside on_connection_closed")
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            print("Connection closed, reopening in 5 secs")
# These require stop_ioloop_on_close=False in connect()
# This does not seem to work at this time, maybe I'm missing a piece?
# Seems to work now.
            self._connection.add_timeout(5, self._connection.ioloop.stop)
# This does work (given stop_ioloop_on_close=False of course)
# But should probably be replaced with stop_ioloop_on_close=True and a raw python loop that sleeps and attempts reconnects
#            self._connection.add_timeout(5, self.run)

    def open_channel(self):
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        if self._exchange_name == '':
            # Using the default exchange, so skip setup_exchange().
            self.setup_queue()
        else:
            self.setup_exchange()

    def on_channel_closed(self, channel, reply_code, reply_text):
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def setup_exchange(self):
        self._channel.exchange_declare(
                                       callback=self.on_exchange_declare_ok,
                                       exchange=self._exchange_name,
                                       exchange_type=self._exchange_type
                                       )

    def on_exchange_declare_ok(self, _unused):
        self.setup_queue()

    def setup_queue(self):
        self._channel.queue_declare(
                                    queue=self._queue,
                                    callback=self.on_queue_declare_ok
                                    )

    def on_queue_declare_ok(self, _unused):
        if self._exchange_name == '':
            # Using the default exchange, so skip queue binding.
            self.start_publishing()
        else:
            self._channel.queue_bind(
                                     callback=self.on_queue_bind_ok,
                                     exchange=self._exchange_name,
                                     queue=self._queue,
                                     routing_key=self._routing_key
                                     )

    def on_queue_bind_ok(self, _unused):
        self.start_publishing()

    def start_publishing(self):
        self.schedule_next_message()

    def schedule_next_message(self):
        self._connection.add_timeout(self.PUBLISH_INTERVAL,
                                     self.publish_message)

    def publish_message(self):
        if self._channel is None or not self._channel.is_open:
            return

        # TODO: Ensure message is delivered, if not keep in _msg and try to re-deliver.
        # -So it appears as if messages are not getting dropped, but might be racey.
        # -We could set self._msg = None after a message is published and then only
        #  assign self._msg if self._msg == None. This way unpublished messages will
        #  be held onto and not assigned over.
        self._msg = self._messages.get().rstrip('\r\n')
        self._channel.basic_publish(
                                    exchange=self._exchange_name,
                                    routing_key=self._routing_key,
                                    body=self._msg
                                    )
        self.schedule_next_message()

    def stop_and_close(self):
        self._stopping = True
        if self._channel is not None:
            self._channel.close()
        if self._connection is not None:
            self._connection.close()
        if (self._connection is not None and not self._connection.is_closed):
            # Finish closing.
            self._connection.ioloop.start()

    def run(self):
        while not self._stopping:
            self._connection = None
            try:
                self._connection = self.connect()
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop_and_close()
### END: class RMQ_Publisher()

def enqueue_messages(queue):
    for line in fileinput.input():
        queue.put(line)
### END: enqueue_messages()

def get_configuration():
    f_self = basename(sys.argv[0])
    f_config = 'qsettings.yaml'

    # Pika expects string type for most arguments (e.g. exchange, queue, etc.)
    # and will error if receiving an int type. However, a numerically-named
    # exchange, queue, etc. is perfectly valid (e.g. an exchange named '123').
    # If the user does not quote the <value> in a <key>:<value> pair,
    # a numeric-only <value> will be imported as type int, causing calls to
    # Pika to fail if passed this value directly. So just cast to str() right?
    # Not so fast...
    #
    # In Python numbers with leading zeros are interpreted as octal. Therefore
    # a <value> of '0100' becomes '64'. Probably not what the user expects when
    # they set 'exchange_name: 0100' (without quotes) in the config file.
    # In Python3 the notation for octals has changed and it will bail/error
    # entirely on numbers with leading zeros (e.g. calling 'str(0100)').
    # At this point we're at a dead end, left with instructing the user via
    # comments/documentation to make sure to quote values in the config file if
    # the value consists of only numerals. But actual strings won't require
    # quotes. Fundamentally we're now burdening the user (and more error prone)
    # with our inconsistent behavior/requirements.
    #
    # Thankfully there are tricks we can do with the PyYAML loader. If we tell
    # PyYAML to use the BaseLoader, e.g.:
    #   yaml.load(stream, Loader=yaml.loader.BaseLoader)
    # ...all keys and values are imported as strings! Solved! Wait, no, there's
    # still a problem...
    #
    # In PyYAML <4.1 yaml.load is not safe, as <key>:<value> pair like this
    # demonstrates:
    #   favorite: !!python/object/apply:os.system ['echo hello']
    #
    # And while the BaseLoader doesn't appear to be vulnerable to this one
    # particular scenario, I'm still uneasy about it as it still appears to
    # attempt to process this on some level.
    #
    # There's one last problem with the BaseLoader. Empty values will be
    # imported as empty strings ('') instead of None/NULL, which is not what is
    # desired in this case.
    #
    # So how can we use the SafeLoader (yaml.safe_load()) and still have all
    # <key>:<value> pairs imported as strings? Looking at the PyYAML source
    # code, the default resolver for scalar nodes is type str! So if we find a
    # way to clear all the implicit resolver mappings PyYAML's str constructor
    # will always be used!
    #
    # The implicit resolver mappings are stored in the dictionary
    # yaml.SafeLoader.yaml_implicit_resolvers, so all we need to do
    # is call .clear() on it. Boom, all keys and values are imported as
    # strings, whether quoted or not AND a <value> with leading zeros such as
    # '0100' (unquoted in the config though) will be imported correctly as
    # '0100' instead of '64'. w00t!
    yaml.SafeLoader.yaml_implicit_resolvers.clear()

    # Just need to add back the NULL resolver or blank values will be imported
    # as an empty string (''). Copied straight from the PyYAML source code.
    yaml.SafeLoader.add_implicit_resolver(
        'tag:yaml.org,2002:null',
        re.compile(r'''^(?: ~
                    |null|Null|NULL
                    | )$''', re.X),
                    ['~', 'n', 'N', ''])

    with open(f_config, 'r') as stream:
        try:
            s = yaml.safe_load(stream)
        except yaml.YAMLError as error:
            sys.exit('ERROR: %s: Loading configuration file failed:\n%s'
                     % (f_self, error))

    required_keys = [ 'host', 'port', 'username', 'password', 'exchange_name',
                      'exchange_type', 'queue', 'routing_key', 'ssl' ]

    # Set exchange_type='direct' if exchange_name==''.
    if not 'exchange_name' in s:
        sys.exit('ERROR: %s: Required key/setting \'exchange_name\' not set.\n'
                 'ERROR: Check the configuration file (%s).'
                 % (f_self, f_config))
    if s['exchange_name'] == '':
        s['exchange_type'] = 'direct'
        required_keys.remove('exchange_type') # Skip unnecessary checking below.

    for key in required_keys:
        if not key in s or s[key] == None:
            sys.exit('ERROR: %s: Required key/setting \'%s\' not set.\n'
                     'ERROR: Check the configuration file (%s).'
                     % (f_self, key, f_config))
        # FIXME: DEBUGGING else. Remove sometime.
        else:
            print('s[{}]:'.format(key).ljust(19)
                    + '[{}]'.format(s[key]).ljust(16)
                    + '{}  {}'.format(str(type(key))[6:-1],str(type(s[key]))[6:-1])
                    )

    try:
        if not 1 <= int(s['port']) <= 65535:
            raise
    except:
        sys.exit('ERROR: %s: Port \'%s\' is not a valid port number.\n'
                 'ERROR: Check the configuration file (%s).'
                 % (f_self, s['port'], f_config))

    return s
### END: get_configuration()

def main():
    s = get_configuration()

    queued_messages = Queue()
    enqueue_messages_t = Thread(target=enqueue_messages,
                                args=(queued_messages,))
    enqueue_messages_t.start()

    # TODO/NOTE: We probably won't want to pass all of 's' once/if it contains
    #            "other" settings not related to pika/rabbitmq/the publisher.
    RMQ_Publish = RMQ_Publisher(s, queued_messages)
    RMQ_Publish.run()
### END: main()

if __name__ == '__main__':
    main()

