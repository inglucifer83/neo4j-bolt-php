<?php

/*
 * This file is part of the GraphAware Bolt package.
 *
 * (c) Graph Aware Limited <http://graphaware.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GraphAware\Bolt;

use GraphAware\Bolt\Exception\IOException;
use GraphAware\Bolt\IO\StreamSocket;
use GraphAware\Bolt\Protocol\SessionRegistry;
use GraphAware\Bolt\PackStream\Packer;
use GraphAware\Bolt\Protocol\V1\Session;
use GraphAware\Common\Driver\DriverInterface;
use Symfony\Component\EventDispatcher\EventDispatcher;
use GraphAware\Bolt\Exception\HandshakeException;

class Driver implements DriverInterface
{
    const VERSION = '1.5.4';

    const DEFAULT_TCP_PORT = 7687;

    /**
     * @var StreamSocket
     */
    protected $io;

    /**
     * @var StreamSocket
     */
    protected $leaderIO;

    /**
     * @var EventDispatcher
     */
    protected $dispatcher;

    /**
     * @var SessionRegistry
     */
    protected $sessionRegistry;

    /**
     * @var SessionRegistry
     */
    protected $leaderSessionRegistry;

    /**
     * @var bool
     */
    protected $versionAgreed = false;

    /**
     * @var bool
     */
    protected $leaderVersionAgreed = false;

    /**
     * @var Session
     */
    protected $session;

    /**
     * @var Session
     */
    protected $leaderSession;

    /**
     * @var array
     */
    protected $credentials;

    /**
     * @var bool Determines whether it is a causal cluster that we're dealing with
     */
    protected $isCausalCluster;

    /**
     * @var array Servers we can execute write queries on.
     */
    private $writeServers = [];

    /**
     * @var array Servers we can execute read queries on.
     */
    private $readServers = [];

    /**
     * @return string
     */
    public static function getUserAgent()
    {
        return 'GraphAware-BoltPHP/'.self::VERSION;
    }

    /**
     * @param string             $uri
     * @param Configuration|null $configuration
     */
    public function __construct($uri, Configuration $configuration = null)
    {
        $this->credentials = null !== $configuration ? $configuration->getValue('credentials', []) : [];
        /*
        $ctx = stream_context_create(array());
        define('CERTS_PATH',
        '/Users/ikwattro/dev/_graphs/3.0-M02-NIGHTLY/conf');
        $ssl_options = array(
            'cafile' => CERTS_PATH . '/cacert.pem',
            'local_cert' => CERTS_PATH . '/ssl/snakeoil.pem',
            'peer_name' => 'example.com',
            'allow_self_signed' => true,
            'verify_peer' => true,
            'capture_peer_cert' => true,
            'capture_peer_cert_chain' => true,
            'disable_compression' => true,
            'SNI_enabled' => true,
            'verify_depth' => 1
        );
        foreach ($ssl_options as $k => $v) {
            stream_context_set_option($ctx, 'ssl', $k, $v);
        }
        */

        $config = null !== $configuration ? $configuration : Configuration::create();
        $parsedUri = parse_url($uri);
        $host = isset($parsedUri['host']) ? $parsedUri['host'] : $parsedUri['path'];
        $port = isset($parsedUri['port']) ? $parsedUri['port'] : static::DEFAULT_TCP_PORT;
        $this->dispatcher = new EventDispatcher();
        $this->io = StreamSocket::withConfiguration($host, $port, $config, $this->dispatcher);
        $this->sessionRegistry = new SessionRegistry($this->io, $this->dispatcher);
        $this->sessionRegistry->registerSession(Session::class);

        // get role to see whether we should get a writer session
        // or this one is already connected to a leader

        // 1. determine whether it is a causal cluster
        if ($this->isCausalCluster()) {
            // 2. create a writer session, now that we have writer and reader servers
            // 2.1. choose a writer server (supposedly the leader, could always be only one [unsure])
            $writeServer = $this->writeServers[array_rand($this->writeServers)];

            // 2.2. initiate an io writer and feed it to a new chunk writer to send request
            $parsedWriteUri = parse_url($writeServer);
            $leaderHost = isset($parsedWriteUri['host']) ? $parsedWriteUri['host'] : $parsedWriteUri['path'];
            $leaderPort = isset($parsedWriteUri['port']) ? $parsedWriteUri['port'] : static::DEFAULT_TCP_PORT;
            $this->leaderIO = StreamSocket::withConfiguration($leaderHost, $leaderPort, $config, $this->dispatcher);
            $this->leaderSessionRegistry = new SessionRegistry($this->leaderIO, $this->dispatcher);
            $this->leaderSessionRegistry->registerSession(Session::class);
        }
    }

    /**
     * Determines whether this is a causal cluster
     * that we are connected to.
     *
     * @return bool
     */
    public function isCausalCluster()
    {
        // at first, assume it is not a cluster until proven to be so.
        if (!isset($this->isCausalCluster)) {
            $this->isCausalCluster = false;
            // do we have the cluster procedure? (available in enterprise and only in clusters)
            $proceduresResult = $this->session()->run('CALL dbms.procedures() YIELD name WHERE name="dbms.cluster.routing.getServers" RETURN name');

            if (count($proceduresResult->getRecords()) > 0) {
                $result = $this->session()->run('CALL dbms.cluster.routing.getServers()');

                // determine whether we're using clustering
                if (count($result->getRecords()) > 0 && $result->getRecord()->hasValue('servers')) {
                    $this->isCausalCluster = true;
                    // distribute servers
                    foreach ($result->getRecord()->value('servers') as $server) {
                        switch ($server['role']) {
                            case 'WRITE':
                                $this->writeServers = $server['addresses'];
                                break;
                            case 'READ':
                                $this->readServers = $server['addresses'];
                                break;
                        }
                    }
                }
            }
        }

        return $this->isCausalCluster;
    }

    /**
     * @return Session
     */
    public function session()
    {
        if (null !== $this->session) {
            return $this->session;
        }

        if (!$this->versionAgreed) {
            $this->versionAgreed = $this->handshake();
        }

        $this->session = $this->sessionRegistry->getSession($this->versionAgreed, $this->credentials);

        return $this->session;
    }

    public function writeSession() {
        if (!$this->isCausalCluster) {
            return $this->session();
        }

        if (null !== $this->leaderSession) {
            return $this->leaderSession;
        }

        if (!$this->leaderVersionAgreed) {
            $this->leaderVersionAgreed = $this->handshake($this->leaderIO);
        }

        $this->leaderSession = $this->leaderSessionRegistry->getSession($this->leaderVersionAgreed, $this->credentials);

        return $this->leaderSession;
    }

    /**
     * @return int
     *
     * @throws HandshakeException
     */
    public function handshake($io = null)
    {
        if (!$io) {
            $io = $this->io;
        }
        $packer = new Packer();

        if (!$io->isConnected()) {
            $io->reconnect();
        }

        $msg = '';
        $msg .= chr(0x60).chr(0x60).chr(0xb0).chr(0x17);

        foreach (array(1, 0, 0, 0) as $v) {
            $msg .= $packer->packBigEndian($v, 4);
        }

        try {
            $io->write($msg);
            $rawHandshakeResponse = $io->read(4);
            $response = unpack('N', $rawHandshakeResponse);
            $version = $response[1];

            if (0 === $version) {
                $this->throwHandshakeException(sprintf('Handshake Exception. Unable to negotiate a version to use. Proposed versions were %s',
                    json_encode(array(1, 0, 0, 0))));
            }

            return $version;
        } catch (IOException $e) {
            $this->throwHandshakeException($e->getMessage());
        }
    }

    /**
     * @param string $message
     */
    private function throwHandshakeException($message)
    {
        throw new HandshakeException($message);
    }
}
