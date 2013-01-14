<?php

namespace MsgQueue;

class Connection
{
	public static $connection;

	public function __construct($hostname = '127.0.0.1')
	{
        if (empty(self::$connection)) {

            self::$connection = new \Pheanstalk($hostname);

			$listening = self::$connection->getConnection()->isServiceListening();
			if (!$listening) {
				throw new \Exception('Message queue server is not available');
			}

        }

        return self::$connection;
	}

	public function put($tube, $data)
	{
		self::$connection->useTube($tube)->put($data);
	}

	public function bury($job)
	{
		self::$connection->bury($job);
	}

	public function reserve($tube)
	{
		return self::$connection->watch($tube)->ignore('default')->reserve();
	}

	public function delete($job)
	{
		self::$connection->delete($job);
	}

	public function statsTube($name)
	{
		return self::$connection->statsTube($name);
	}

}