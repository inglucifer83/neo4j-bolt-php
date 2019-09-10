<?php

namespace GraphAware\Bolt\Tests;

use GraphAware\Bolt\GraphDatabase;

class IntegrationTestCase extends \PHPUnit_Framework_TestCase
{
    /**
     * @var \GraphAware\Bolt\Driver
     */
    protected $driver;

    /**
     * @inheritdoc
     */
    protected function setUp()
    {
        $this->driver = GraphDatabase::driver("bolt://localhost");
    }

    /**
     * @return \GraphAware\Bolt\Driver
     */
    protected function getDriver()
    {
        return $this->driver;
    }

    /**
     * @return \Graphaware\Bolt\Protocol\SessionInterface
     */
    protected function getSession()
    {
        return $this->driver->session();
    }

    /**
     * @return \Graphaware\Bolt\Protocol\SessionInterface
     */
    protected function getWriteSession()
    {
        return $this->driver->writeSession();
    }

    /**
     * Empty the database
     */
    protected function emptyDB()
    {
        $this->getWriteSession()->run('MATCH (n) DETACH DELETE n');
    }
}
