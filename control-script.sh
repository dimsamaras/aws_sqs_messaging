#!/bin/sh
# from http://blog.scphillips.com/posts/2013/07/getting-a-python-script-to-run-in-the-background-as-a-service-on-boot/
# as sawn https://thingsmatic.com/2016/06/18/daemonize-that-python-script/

### BEGIN INIT INFO
# Provides:          messageReceiver
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: A service for receiving messages from sqs queue
# Description:       A service for receiving messages from sqs queue
### END INIT INFO

# DIR=/home/[USER]/HomeAutomation/RemoteControlApp
DIR=/var/python-scripts/aws/sqs
DAEMON=$DIR/messageReceiver.py
DAEMON_NAME=worker
DAEMON_OPTS="--env DEV_2 work"

# Root is needed to use the GPIO pins on the Raspberry
DAEMON_USER=root

PIDFILE=/var/run/$DAEMON_NAME.pid

. /lib/lsb/init-functions

do_start () {
    log_daemon_msg "Starting system $DAEMON_NAME daemon"
    start-stop-daemon --start --background --pidfile $PIDFILE --make-pidfile --user $DAEMON_USER --chuid $DAEMON_USER --startas $DAEMON $DAEMON_OPTS
    log_end_msg $?
}
do_stop () {
    log_daemon_msg "Stopping system $DAEMON_NAME daemon"
    start-stop-daemon --stop --pidfile $PIDFILE --retry 10
    log_end_msg $?
}

case "$1" in

    start|stop)
        do_${1}
        ;;

    restart|reload|force-reload)
        do_stop
        do_start
        ;;

    status)
        status_of_proc "$DAEMON_NAME" "$DAEMON" && exit 0 || exit $?
        ;;

    *)
        echo "Usage: /etc/init.d/$DAEMON_NAME {start|stop|restart|status}"
        exit 1
        ;;

esac
exit 0