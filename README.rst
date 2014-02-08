Pesand
==================

MQTT 3.1 broker which supports

- QoS 0
- Retain

How to use
--------------------

Install
++++++++++

Download a binary of your architecture from dron.io.

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


Naming
--------------

'pesan' is a 'message' in Indonesian. plus "d" means daemon.


The giants on whose shoulders this works stands
------------------------------------------------------------------

- https://github.com/jeffallen/mqtt

  The original architecture is based on jeffallen's code. So Many
  thanks.  I just re-structured and added some of improvement.

- https://github.com/hunin/mqtt

  MQTT Protocol itself is used this code. Thank you!
