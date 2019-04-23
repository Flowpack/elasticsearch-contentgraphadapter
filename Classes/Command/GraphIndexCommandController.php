<?php

namespace Flowpack\ElasticSearch\ContentGraphAdapter\Command;

/*
 * This file is part of the CORE4.Neos.ElasticSearch.ContentGraphAdapter package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */
use CORE4\Neos\ElasticSearch\ContentGraphAdapter\Indexer\NodeIndexer;
use CORE4\Neos\ElasticSearch\ContentGraphAdapter\Mapping\NodeTypeMappingBuilder;
use Flowpack\ElasticSearch\Transfer\Exception\ApiException;
use Neos\Flow\Configuration\ConfigurationManager;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\ContentRepository\InMemoryGraph\GraphService;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;
use Neos\ContentRepository\Domain as ContentRepository;

/**
 * Provides CLI features for graph index handling
 *
 * @Flow\Scope("singleton")
 */
class GraphIndexCommandController extends CommandController
{
    /**
     * @Flow\Inject
     * @var NodeIndexer
     */
    protected $nodeIndexer;

    /**
     * @Flow\Inject
     * @var ContentRepository\Repository\NodeDataRepository
     */
    protected $nodeDataRepository;

    /**
     * @Flow\Inject
     * @var NodeTypeMappingBuilder
     */
    protected $nodeTypeMappingBuilder;

    /**
     * @Flow\Inject
     * @var \Flowpack\ElasticSearch\ContentRepositoryAdaptor\LoggerInterface
     */
    protected $logger;

    /**
     * @Flow\Inject
     * @var ConfigurationManager
     */
    protected $configurationManager;

    /**
     * @Flow\Inject
     * @var GraphService
     */
    protected $graphService;

    /**
     * @var array
     */
    protected $settings;

    /**
     * Called by the Flow object framework after creating the object and resolving all dependencies.
     *
     * @param integer $cause Creation cause
     */
    public function initializeObject($cause)
    {
        if ($cause === ObjectManagerInterface::INITIALIZATIONCAUSE_CREATED) {
            $this->settings = $this->configurationManager->getConfiguration(ConfigurationManager::CONFIGURATION_TYPE_SETTINGS, 'Neos.ContentRepository.Search');
        }
    }

    /**
     * Index all nodes by creating a new index and when everything was completed, switch the index alias.
     *
     * This command (re-)indexes all nodes contained in the content repository and sets the schema beforehand.
     *
     * @param integer $limit Amount of nodes to index at maximum
     * @param boolean $update if TRUE, do not throw away the index at the start. Should *only be used for development*.
     * @param string $workspace name of the workspace which should be indexed
     * @param string $postfix Index postfix, index with the same postifix will be deleted if exist
     * @return void
     */
    public function buildCommand($limit = null, $update = false, $workspace = null, $postfix = null)
    {
        if ($update === true) {
            $this->logger->log('!!! Update Mode (Development) active!', LOG_INFO);
        } else {
            $this->nodeIndexer->setIndexNamePostfix($postfix ?: time());
            if ($this->nodeIndexer->getIndex()->exists() === true) {
                $this->logger->log(sprintf('Deleted index with the same postfix (%s)!', $postfix), LOG_WARNING);
                $this->nodeIndexer->getIndex()->delete();
            }
            $this->nodeIndexer->getIndex()->create();
            $this->logger->log('Created index ' . $this->nodeIndexer->getIndexName(), LOG_INFO);

            $nodeTypeMappingCollection = $this->nodeTypeMappingBuilder->buildMappingInformation($this->nodeIndexer->getIndex());
            foreach ($nodeTypeMappingCollection as $mapping) {
                /** @var \Flowpack\ElasticSearch\Domain\Model\Mapping $mapping */
                $mapping->apply();
            }
            $this->logger->log('Updated Mapping.', LOG_INFO);
        }

        $this->logger->log(sprintf('Indexing %snodes ... ', ($limit !== null ? 'the first ' . $limit . ' ' : '')), LOG_INFO);

        $this->outputLine('Initializing content graph...');
        $query = $this->nodeDataRepository->createQuery();
        $numberOfNodes = $query->matching($query->equals('workspace', 'live'))->count();
        $graph = $this->graphService->getContentGraph($this->output);

        $this->outputLine('Indexing content graph...');
        $time = time();
        $this->output->progressStart(count($graph->getNodes()));
        $indexedEdges = 0;
        $nodesSinceLastFlush = 0;
        foreach ($graph->getNodes() as $node) {
            $this->nodeIndexer->indexGraphNode($node);
            $nodesSinceLastFlush++;
            $indexedEdges += count($node->getIncomingEdges());
            $this->output->progressAdvance();
            if ($nodesSinceLastFlush >= 100) {
                $this->nodeIndexer->flush();
                $nodesSinceLastFlush = 0;
            }
        }
        $this->output->progressFinish();
        $this->nodeIndexer->flush();
        $timeSpent = time() - $time;
        $this->logger->log('Done. Indexed ' . count($graph->getNodes()) . ' nodes and ' . $indexedEdges . ' edges in ' . $timeSpent . ' s at ' . round(count($graph->getNodes()) / $timeSpent) . ' nodes/s (' . round($indexedEdges / $timeSpent) . ' edges/s)',
            LOG_INFO);
        $this->nodeIndexer->getIndex()->refresh();

        // TODO: smoke tests
        if ($update === false) {
            $this->nodeIndexer->updateIndexAlias();
        }
    }

    /**
     * Clean up old indexes (i.e. all but the current one)
     *
     * @return void
     */
    public function cleanupCommand()
    {
        try {
            $indicesToBeRemoved = $this->nodeIndexer->removeOldIndices();
            if (count($indicesToBeRemoved) > 0) {
                foreach ($indicesToBeRemoved as $indexToBeRemoved) {
                    $this->logger->log('Removing old index ' . $indexToBeRemoved);
                }
            } else {
                $this->logger->log('Nothing to remove.');
            }
        } catch (ApiException $exception) {
            $response = json_decode($exception->getResponse());
            var_dump($response->status, $response->error);
            $this->logger->log(sprintf('Nothing removed. ElasticSearch responded with status %s, saying "%s"', $response->status, $response->error));
        }
    }
}
