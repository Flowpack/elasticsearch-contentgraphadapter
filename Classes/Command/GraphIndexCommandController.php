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
use Flowpack\ElasticSearch\ContentGraphAdapter\Indexer\NodeIndexer;
use Flowpack\ElasticSearch\ContentGraphAdapter\Mapping\NodeTypeMappingBuilder;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception as CRAException;
use Psr\Log\LoggerInterface;
use Flowpack\ElasticSearch\Domain\Model\Mapping;
use Flowpack\ElasticSearch\Transfer\Exception\ApiException;
use Neos\Flow\Configuration\ConfigurationManager;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\ContentRepository\InMemoryGraph\ContentSubgraph\GraphService;
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
     * @var LoggerInterface
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
     * @Flow\InjectConfiguration(path="indexing.batchSize")
     * @var int
     */
    protected $batchSize;

    /**
     * @Flow\InjectConfiguration(path="indexing.displayProgress")
     * @var bool
     */
    protected $displayProgress;

    /**
     * @var array
     */
    protected $settings;

    public function initializeObject($cause)
    {
        if ($cause === ObjectManagerInterface::INITIALIZATIONCAUSE_CREATED) {
            $this->settings = $this->configurationManager->getConfiguration(ConfigurationManager::CONFIGURATION_TYPE_SETTINGS,
                'Neos.ContentRepository.Search');
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
     * @param bool $displayProgress
     * @return void
     * @throws ApiException
     * @throws CRAException
     */
    public function buildCommand($limit = null, $update = false, $workspace = null, $postfix = '', bool $displayProgress = false): void
    {
        $displayProgress = $displayProgress || $this->displayProgress;
        $this->createNewIndex($postfix);
        $this->applyMapping();

        $this->logger->info(sprintf('Indexing %snodes ... ', ($limit !== null ? 'the first ' . $limit . ' ' : '')));

        $this->outputLine('Initializing content graph...');
        if ($displayProgress) {
            $graph = $this->graphService->getContentGraph($this->output);
        } else {
            $graph = $this->graphService->getContentGraph();
        }

        $this->outputLine('Indexing content graph...');
        $time = time();
        if ($displayProgress) {
            $this->output->progressStart(count($graph->getNodes()));
        }
        $indexedHierarchyRelations = 0;
        $nodesSinceLastFlush = 0;
        $this->nodeIndexer->setContentGraph($graph);
        foreach ($graph->getNodes() as $node) {
            $this->nodeIndexer->indexGraphNode($node);
            $nodesSinceLastFlush++;
            $indexedHierarchyRelations += count($node->getIncomingHierarchyRelations());
            if ($displayProgress) {
                $this->output->progressAdvance();
            }
            if ($nodesSinceLastFlush >= $this->batchSize) {
                $this->nodeIndexer->flush();
                $nodesSinceLastFlush = 0;
            }
        }
        $this->output->progressFinish();
        if ($displayProgress) {
            $this->nodeIndexer->flush();
        }
        $timeSpent = time() - $time;
        $this->logger->info('Done. Indexed ' . count($graph->getNodes()) . ' nodes and ' . $indexedHierarchyRelations . ' edges in ' . $timeSpent . ' s at ' . round(count($graph->getNodes()) / $timeSpent) . ' nodes/s (' . round($indexedHierarchyRelations / $timeSpent) . ' edges/s)');
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
     * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception
     */
    public function cleanupCommand()
    {
        try {
            $indicesToBeRemoved = $this->nodeIndexer->removeOldIndices();
            if (count($indicesToBeRemoved) > 0) {
                foreach ($indicesToBeRemoved as $indexToBeRemoved) {
                    $this->logger->info('Removing old index ' . $indexToBeRemoved);
                }
            } else {
                $this->logger->info('Nothing to remove.');
            }
        } catch (ApiException $exception) {
            $response = json_decode($exception->getResponse());
            $this->logger->error(sprintf('Nothing removed. ElasticSearch responded with status %s, saying "%s"',
                $response->status, $response->error));
        }
    }

    /**
     * Create a new index with the given $postfix.
     *
     * @param string $postfix
     * @return void
     * @throws CRAException
     */
    protected function createNewIndex(string $postfix): void
    {
        $this->nodeIndexer->setIndexNamePostfix($postfix ?: (string)time());
        if ($this->nodeIndexer->getIndex()->exists() === true) {
            $this->logger->warning(sprintf('Deleted index with the same postfix (%s)!', $postfix));
            $this->nodeIndexer->getIndex()->delete();
        }
        $this->nodeIndexer->getIndex()->create();
    }

    /**
     * Apply the mapping to the current index.
     *
     * @return void
     * @throws CRAException
     */
    protected function applyMapping(): void
    {
        $nodeTypeMappingCollection = $this->nodeTypeMappingBuilder->buildMappingInformation($this->nodeIndexer->getIndex());
        foreach ($nodeTypeMappingCollection as $mapping) {
            /** @var Mapping $mapping */
            $mapping->apply();
        }
        $this->logger->info('Updated Mapping.');
    }
}
