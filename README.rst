Pesand
==================

MQTT 3.1 broker which supports

- QoS 0
- topic wildcard
- Retain

How to use
--------------------

Install
++++++++++

Download a binary of your architecture from dron.io.

- https://drone.io/github.com/shirou/pesand/files

or build by yourself.

::

  % go get github.com/shirou/pesand

Use
+++++

::

  % pesand -c pesand.conf


Configration
-----------------

Because this is not stable, not documented yet.


Future work
---------------------

- QoS 1, 2
- Will messages
- LevelDB persistent storage
- store sent messages
- durable subscribe
- websocket


Naming
--------------

'pesan' is a 'message' in Indonesian. plus "d" means daemon.


The giants on whose shoulders this works stands
------------------------------------------------------------------

- https://github.com/jeffallen/mqtt

  The original architecture is based on jeffallen's code.  I just
  re-structured and added some of improvement. Thank you.

- https://github.com/hunin/mqtt

  MQTT Protocol itself is used this code. Thank you.
