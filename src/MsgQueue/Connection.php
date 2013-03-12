<?php

namespace MsgQueue;

class Connection
{
    public static $connection;

    public function __construct($hostname = '127.0.0.1')
    {
        if (empty(self::$connection)) {
            self::$connection = new \Pheanstalk_Pheanstalk($hostname);
            $listening = self::$connection->getConnection()->isServiceListening();
            if (!$listening) {
                throw new \Exception('Message queue server is not available');
            }
        }

        return self::$connection;
    }

    public function put($tube, $data)
    {
        self::$connection->useTube($tube)->put($data, 0);
    }

    public function bury($job)
    {
        self::$connection->bury($job);
    }

    public function kick($tube, $job_count = 1)
    {
    	return self::$connection->useTube($tube)->kick($job_count);
    }

    public function reserve($tube)
    {
        return self::$connection->watch($tube)->ignore('default')->reserve();
    }

    public function release($job, $delay = 0)
    {
        self::$connection->release($job, 0, $delay);
    }

    public function delete($job)
    {
        self::$connection->delete($job);
    }

    public function deleteAll($tube)
    {
	    // TODO Complain to the makers of Pheanstalk to see if they can replace the notice for an exception

		try {

		    while ($job = self::$connection->useTube($tube)->peekDelayed()) {
		        self::$connection->delete($job);
		    }

		} catch (Pheanstalk_Exception_ConnectionException $e) {}

		try {

		    while ($job = self::$connection->useTube($tube)->peekBuried()) {
		        self::$connection->delete($job);
		    }

		} catch (Pheanstalk_Exception_ConnectionException $e) {}

		try {

		    while ($job = self::$connection->useTube($tube)->peekReady()) {
		        self::$connection->delete($job);
		    }

		} catch (Pheanstalk_Exception_ConnectionException $e) {}

    }

    public function statsTube($name)
    {
    	return self::$connection->statsTube($name);
    }
}
