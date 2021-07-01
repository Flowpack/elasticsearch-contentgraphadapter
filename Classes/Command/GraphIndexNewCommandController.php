<?php
declare(strict_types=1);

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

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Service\DimensionsService;
use Neos\ContentRepository\DimensionSpace\Dimension\ContentDimensionIdentifier;
use Neos\Flow\Annotations as Flow;
use Doctrine\Common\Collections\ArrayCollection;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\IndexDriverInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\NodeTypeMappingBuilderInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\ElasticSearchClient;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\ErrorHandling\ErrorHandlingService;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception\ConfigurationException;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception\RuntimeException;
use Flowpack\ElasticSearch\ContentGraphAdapter\Indexer\NodeIndexer;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\WorkspaceIndexer;
use Flowpack\ElasticSearch\Domain\Model\Mapping;
use Flowpack\ElasticSearch\Transfer\Exception\ApiException;
use Neos\ContentRepository\DimensionSpace\DimensionSpace\DimensionSpacePoint;
use Neos\ContentRepository\Domain\Model\Workspace;
use Neos\ContentRepository\Domain\Repository\NodeDataRepository;
use Neos\ContentRepository\Domain\Repository\WorkspaceRepository;
use Neos\ContentRepository\Domain\Service\ContentDimensionCombinator;
use Neos\ContentRepository\Domain\Service\ContentDimensionPresetSourceInterface;
use Neos\ContentRepository\Domain\Service\Context;
use Neos\ContentRepository\Domain\Service\ContextFactoryInterface;
use Neos\ContentRepository\InMemoryGraph\ContentSubgraph\ContentGraph;
use Neos\ContentRepository\InMemoryGraph\ContentSubgraph\ContentSubgraph;
use Neos\ContentRepository\InMemoryGraph\ContentSubgraph\GraphService;
use Neos\ContentRepository\DimensionSpace\DimensionSpace\ContentDimensionZookeeper;
use Neos\Flow\Cli\CommandController;
use Neos\Flow\Cli\Exception\StopCommandException;
use Neos\Flow\Configuration\ConfigurationManager;
use Neos\Flow\Core\Booting\Exception\SubProcessException;
use Neos\Flow\Core\Booting\Scripts;
use Neos\Flow\Log\Utility\LogEnvironment;
use Neos\Utility\Files;
use Flowpack\ElasticSearch\ContentGraphAdapter\Mapping\NodeTypeMappingBuilder;
use Psr\Log\LoggerInterface;

/**
 * Provides CLI features for index handling
 *
 * @Flow\Scope("singleton")
 */
class GraphIndexNewCommandController extends CommandController
{
    /**
     * @Flow\InjectConfiguration(package="Neos.Flow")
     * @var array
     */
    protected $flowSettings;

    /**
     * @var array
     * @Flow\InjectConfiguration(package="Neos.ContentRepository.Search")
     */
    protected $settings;

    /**
     * @var bool
     * @Flow\InjectConfiguration(path="command.useSubProcesses", package="Flowpack.ElasticSearch.ContentRepositoryAdaptor")
     */
    protected $useSubProcesses = true;

    /**
     * @Flow\Inject
     * @var ErrorHandlingService
     */
    protected $errorHandlingService;

    /**
     * @Flow\Inject
     * @var NodeIndexer
     */
    protected $nodeIndexer;

    /**
     * @Flow\Inject
     * @var WorkspaceRepository
     */
    protected $workspaceRepository;

    /**
     * @Flow\Inject
     * @var NodeDataRepository
     */
    protected $nodeDataRepository;

    /**
     * @Flow\Inject
     * @var ContentDimensionPresetSourceInterface
     */
    protected $contentDimensionPresetSource;

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
     * @var ContextFactoryInterface
     */
    protected $contextFactory;

    /**
     * @Flow\Inject
     * @var ContentDimensionCombinator
     */
    protected $contentDimensionCombinator;

    /**
     * @Flow\Inject
     * @var WorkspaceIndexer
     */
    protected $workspaceIndexer;

    /**
     * @Flow\Inject
     * @var ElasticSearchClient
     */
    protected $searchClient;

    /**
     * @var IndexDriverInterface
     * @Flow\Inject
     */
    protected $indexDriver;

    /**
     * @Flow\Inject
     * @var GraphService
     */
    protected $graphService;


    /**
     * @Flow\Inject
     * @var ContentDimensionZookeeper
     */
    protected $contentDimensionZookeeper;

    /**
     * @var DimensionsService
     * @Flow\Inject
     */
    protected $dimensionsService;

    /**
     * @var boolean
     */
    protected $displayProgress = true;

    /**
     * @var int
     */
    protected $batchSize = 100;

    /**
     * Index all nodes by creating a new index and when everything was completed, switch the index alias.
     *
     * This command (re-)indexes all nodes contained in the content repository and sets the schema beforehand.
     *
     * @param int|null $limit Amount of nodes to index at maximum
     * @param bool $update if TRUE, do not throw away the index at the start. Should *only be used for development*.
     * @param string|null $workspace name of the workspace which should be indexed
     * @param string|null $postfix Index postfix, index with the same postfix will be deleted if exist
     * @return void
     * @throws StopCommandException
     * @throws Exception
     * @throws ConfigurationException
     * @throws ApiException
     */
    public function buildCommand(int $limit = null, bool $update = false, string $workspace = null, string $postfix = null): void
    {
        $this->logger->info(sprintf('Starting elasticsearch indexing %s sub processes', $this->useSubProcesses ? 'with' : 'without'), LogEnvironment::fromMethodName(__METHOD__));

        if ($workspace !== null && $this->workspaceRepository->findByIdentifier($workspace) === null) {
            $this->logger->error('The given workspace (' . $workspace . ') does not exist.', LogEnvironment::fromMethodName(__METHOD__));
            $this->quit(1);
        }

        $postfix = (string)($postfix ?: time());
        $this->nodeIndexer->setIndexNamePostfix($postfix);

        $createIndicesAndApplyMapping = function (array $dimensionsValues) use ($update, $postfix) {
            $this->executeInternalCommand('createInternal', [
                'dimensionsValues' => json_encode($dimensionsValues),
                'update' => $update,
                'postfix' => $postfix,
            ]);
        };

//        $buildIndex = function (array $dimensionsValues) use ($workspace, $limit, $postfix) {
//            $this->build($dimensionsValues, $workspace, $postfix, $limit);
//        };

        $refresh = function (array $dimensionsValues) use ($postfix) {
            $this->executeInternalCommand('refreshInternal', [
                'dimensionsValues' => json_encode($dimensionsValues),
                'postfix' => $postfix,
            ]);
        };

        $updateAliases = function (array $dimensionsValues) use ($update, $postfix) {
            $this->executeInternalCommand('aliasInternal', [
                'dimensionsValues' => json_encode($dimensionsValues),
                'postfix' => $postfix,
                'update' => $update,
            ]);
        };

        $combinations = new ArrayCollection($this->contentDimensionCombinator->getAllAllowedCombinations());

        $runAndLog = function ($command, string $stepInfo) use ($combinations) {
            $timeStart = microtime(true);
            $this->output(str_pad($stepInfo . '... ', 20));
            $combinations->map($command);
            $this->outputLine('<success>Done</success> (took %s seconds)', [number_format(microtime(true) - $timeStart, 2)]);
        };

        $runAndLog($createIndicesAndApplyMapping, 'Creating indices and apply mapping');

        if ($this->aliasesExist() === false) {
            $runAndLog($updateAliases, 'Set up aliases');
        }

        // $runAndLog($buildIndex, 'Indexing nodes');
        $this->outputLine('Initializing content graph...');
        if ($this->displayProgress) {
            $graph = $this->graphService->getContentGraph($this->output);
        } else {
            $graph = $this->graphService->getContentGraph();
        }
        $this->nodeIndexer->setContentGraph($graph);

        $dimensionSpacePointSet = $this->contentDimensionZookeeper->getAllowedDimensionSubspace();
        $workspaceDimensionIdentifier = new ContentDimensionIdentifier('_workspace');
        foreach ($dimensionSpacePointSet as $dimensionSpacePoint) {
            if ($dimensionSpacePoint->getCoordinate($workspaceDimensionIdentifier) === 'live') {
                $coordinates = $dimensionSpacePoint->getCoordinates();
                unset($coordinates['_workspace']);
                $dimensionSpacepointWithoutWorkspace = new DimensionSpacePoint($coordinates);
                $this->buildIndexForDimensionSpacePoint($graph, $dimensionSpacepointWithoutWorkspace, $postfix, $limit);
            }
        }

        $runAndLog($refresh, 'Refresh indicies');
        $runAndLog($updateAliases, 'Update aliases');

        $this->outputLine('Update main alias');
        $this->nodeIndexer->updateMainAlias();

        $this->outputLine();
        $this->outputMemoryUsage();
    }

    /**
     * @return bool
     * @throws ApiException
     * @throws ConfigurationException
     * @throws Exception
     */
    private function aliasesExist(): bool
    {
        $aliasName = $this->searchClient->getIndexName();
        $aliasesExist = false;
        try {
            $aliasesExist = $this->indexDriver->getIndexNamesByAlias($aliasName) !== [];
        } catch (ApiException $exception) {
            // in case of 404, do not throw an error...
            if ($exception->getResponse()->getStatusCode() !== 404) {
                throw $exception;
            }
        }

        return $aliasesExist;
    }

    /**
     * Build up the node index
     *
     * @param ContentGraph $graph
     * @param DimensionSpacePoint $dimensionSpacePoint
     * @param string|null $postfix
     * @param int|null $limit
     * @throws ConfigurationException
     * @throws Exception
     * @throws RuntimeException
     * @throws SubProcessException
     */
    private function buildIndexForDimensionSpacePoint(ContentGraph $graph, DimensionSpacePoint $dimensionSpacePoint, string $postfix, $limit = null): void
    {
        $dimensionsValues = $dimensionSpacePoint->getCoordinates();
        $dimensionsValues = $this->configureNodeIndexer($dimensionsValues, $postfix);

        $this->outputLine("Build index for dimensionValues %s", [json_encode($dimensionsValues)]);


        $timeStart = microtime(true);
        if ($this->displayProgress) {
            $this->output->progressStart(count($graph->getNodes()));
        }

        $nodesIndexed = 0;
        $nodesSinceLastFlush = 0;
        foreach ($graph->getNodes() as $node) {
            $include = false;

            if ($this->displayProgress) {
                $this->output->progressAdvance();
            }

            foreach($node->getIncomingHierarchyRelations() as $hierarchyRelation){
                $relationDimensionsValues = $hierarchyRelation->getSubgraph()->getDimensionSpacePoint()->getCoordinates();
                unset($relationDimensionsValues['_workspace']);
                $relationDimensionsValuesWithoutWorkspace = new DimensionSpacePoint($relationDimensionsValues);
                if ($dimensionSpacePoint->equals($relationDimensionsValuesWithoutWorkspace)) {
                    $include = true;
                }
            }

            if (!$include) {
                continue;
            }

            $this->nodeIndexer->indexGraphNode($node, $dimensionSpacePoint);
            $nodesSinceLastFlush++;
            $nodesIndexed++;

            if ($nodesSinceLastFlush >= $this->batchSize) {
                $this->nodeIndexer->flush();
                $nodesSinceLastFlush = 0;
            }
        }
        $this->nodeIndexer->flush();
        if ($this->displayProgress) {
            $this->output->progressFinish();
        }

        $timeSpent = number_format(microtime(true) - $timeStart, 2);
        $this->outputLine();
        $this->outputLine('<success>Done</success> (took %s seconds, %s nodes indexed)', [$timeSpent, $nodesIndexed]);

        $this->logger->info('Done. Indexed ' . $nodesIndexed . ' nodes in ' . $timeSpent);
        $this->nodeIndexer->getIndex()->refresh();
    }

    /**
     * Internal sub-command to create an index and apply the mapping
     *
     * @param string $dimensionsValues
     * @param bool $update
     * @param string|null $postfix
     * @throws Exception
     * @throws \Flowpack\ElasticSearch\Exception
     * @throws \Neos\Flow\Http\Exception
     * @throws \Exception
     * @Flow\Internal
     */
    public function createInternalCommand(string $dimensionsValues, bool $update = false, string $postfix = null): void
    {
        if ($update === true) {
            $this->logger->warning('!!! Update Mode (Development) active!', LogEnvironment::fromMethodName(__METHOD__));
        } else {
            $dimensionsValuesArray = $this->configureNodeIndexer(json_decode($dimensionsValues, true), $postfix);
            if ($this->nodeIndexer->getIndex()->exists() === true) {
                $this->logger->warning(sprintf('Deleted index with the same postfix (%s)!', $postfix), LogEnvironment::fromMethodName(__METHOD__));
                $this->nodeIndexer->getIndex()->delete();
            }
            $this->nodeIndexer->getIndex()->create();
            $this->logger->info('Created index ' . $this->nodeIndexer->getIndexName() . ' with dimensions ' . json_encode($dimensionsValuesArray), LogEnvironment::fromMethodName(__METHOD__));
        }

        $this->applyMapping();
        $this->outputErrorHandling();
    }

    /**
     * @param string $workspace
     * @param string $dimensionsValues
     * @param string $postfix
     * @param int|null $limit
     * @return void
     * @Flow\Internal
     */
    public function buildWorkspaceInternalCommand(string $workspace, string $dimensionsValues, string $postfix, int $limit = null): void
    {
        $dimensionsValuesArray = $this->configureNodeIndexer(json_decode($dimensionsValues, true), $postfix);

        $workspaceLogger = function ($workspaceName, $indexedNodes, $dimensions) {
            if ($dimensions === []) {
                $message = 'Workspace "' . $workspaceName . '" without dimensions done. (Indexed ' . $indexedNodes . ' nodes)';
            } else {
                $message = 'Workspace "' . $workspaceName . '" and dimensions "' . json_encode($dimensions) . '" done. (Indexed ' . $indexedNodes . ' nodes)';
            }
            $this->outputLine($message);
        };

        $this->workspaceIndexer->indexWithDimensions($workspace, $dimensionsValuesArray, $limit, $workspaceLogger);

        $this->outputErrorHandling();
    }

    /**
     * Internal subcommand to refresh the index
     *
     * @param string $dimensionsValues
     * @param string $postfix
     * @throws Exception
     * @throws \Flowpack\ElasticSearch\Exception
     * @throws \Neos\Flow\Http\Exception
     * @throws ConfigurationException
     * @Flow\Internal
     */
    public function refreshInternalCommand(string $dimensionsValues, string $postfix): void
    {
        $this->configureNodeIndexer(json_decode($dimensionsValues, true), $postfix);

        $this->logger->info(vsprintf('Refreshing index %s', [$this->nodeIndexer->getIndexName()]), LogEnvironment::fromMethodName(__METHOD__));
        $this->nodeIndexer->getIndex()->refresh();

        $this->outputErrorHandling();
    }

    /**
     * @param string $dimensionsValues
     * @param string $postfix
     * @param bool $update
     * @throws Exception
     * @throws \Flowpack\ElasticSearch\Exception
     * @throws ApiException
     * @throws ConfigurationException
     * @Flow\Internal
     */
    public function aliasInternalCommand(string $dimensionsValues, string $postfix, bool $update = false): void
    {
        if ($update === true) {
            return;
        }
        $this->configureNodeIndexer(json_decode($dimensionsValues, true), $postfix);

        $this->logger->info(vsprintf('Update alias for index %s', [$this->nodeIndexer->getIndexName()]), LogEnvironment::fromMethodName(__METHOD__));
        $this->nodeIndexer->updateIndexAlias();
        $this->outputErrorHandling();
    }

    /**
     * @param array $dimensionsValues
     * @param string $postfix
     * @return array
     */
    private function configureNodeIndexer(array $dimensionsValues, string $postfix): array
    {
        $this->nodeIndexer->setIndexNamePostfix($postfix);
        $this->nodeIndexer->setDimensions($dimensionsValues);
        return $dimensionsValues;
    }

    /**
     * Clean up old indexes (i.e. all but the current one)
     *
     * @return void
     * @throws ConfigurationException
     * @throws Exception
     */
    public function cleanupCommand(): void
    {
        $removed = false;
        $combinations = $this->contentDimensionCombinator->getAllAllowedCombinations();
        foreach ($combinations as $dimensionsValues) {
            try {
                $this->nodeIndexer->setDimensions($dimensionsValues);
                $removedIndices = $this->nodeIndexer->removeOldIndices();

                foreach ($removedIndices as $indexToBeRemoved) {
                    $removed = true;
                    $this->logger->info('Removing old index ' . $indexToBeRemoved, LogEnvironment::fromMethodName(__METHOD__));
                }
            } catch (ApiException $exception) {
                $exception->getResponse()->getBody()->rewind();
                $response = json_decode($exception->getResponse()->getBody()->getContents(), false);
                $message = sprintf('Nothing removed. ElasticSearch responded with status %s', $response->status);

                if (isset($response->error->type)) {
                    $this->logger->error(sprintf('%s, saying "%s: %s"', $message, $response->error->type, $response->error->reason), LogEnvironment::fromMethodName(__METHOD__));
                } else {
                    $this->logger->error(sprintf('%s, saying "%s"', $message, $response->error), LogEnvironment::fromMethodName(__METHOD__));
                }
            }
        }
        if ($removed === false) {
            $this->logger->info('Nothing to remove.', LogEnvironment::fromMethodName(__METHOD__));
        }
    }

    private function outputErrorHandling(): void
    {
        if ($this->errorHandlingService->hasError() === false) {
            return;
        }

        $this->outputLine();
        $this->outputLine('<error>%s Errors where returned while indexing. Check your logs for more information.</error>', [$this->errorHandlingService->getErrorCount()]);
    }

    /**
     * @param string $command
     * @param array $arguments
     * @return string
     * @throws RuntimeException
     * @throws SubProcessException
     */
    private function executeInternalCommand(string $command, array $arguments): string
    {
        ob_start(null, 1 << 20);

        if ($this->useSubProcesses) {
            $commandIdentifier = 'flowpack.elasticsearch.contentrepositoryadaptor:nodeindex:' . $command;
            $status = Scripts::executeCommand($commandIdentifier, $this->flowSettings, true, array_filter($arguments));

            if ($status !== true) {
                throw new RuntimeException(vsprintf('Command: %s with parameters: %s', [$commandIdentifier, json_encode($arguments)]), 1426767159);
            }
        } else {
            $commandIdentifier = $command . 'Command';
            call_user_func_array([self::class, $commandIdentifier], $arguments);
        }

        return ob_get_clean();
    }

    /**
     * Create a ContentContext based on the given workspace name
     *
     * @param string $workspaceName Name of the workspace to set for the context
     * @param array $dimensions Optional list of dimensions and their values which should be set
     * @return Context
     */
    private function createContentContext(string $workspaceName, array $dimensions = []): Context
    {
        $contextProperties = [
            'workspaceName' => $workspaceName,
            'invisibleContentShown' => true,
            'inaccessibleContentShown' => true
        ];

        if ($dimensions !== []) {
            $contextProperties['dimensions'] = $dimensions;
            $contextProperties['targetDimensions'] = array_map(static function ($dimensionValues) {
                return array_shift($dimensionValues);
            }, $dimensions);
        }

        return $this->contextFactory->create($contextProperties);
    }

    /**
     * Apply the mapping to the current index.
     *
     * @return void
     * @throws Exception
     * @throws \Flowpack\ElasticSearch\Exception
     * @throws ConfigurationException
     * @throws \Neos\Flow\Http\Exception
     */
    private function applyMapping(): void
    {
        $nodeTypeMappingCollection = $this->nodeTypeMappingBuilder->buildMappingInformation($this->nodeIndexer->getIndex());
        foreach ($nodeTypeMappingCollection as $mapping) {
            /** @var Mapping $mapping */
            $mapping->apply();
        }
    }

    private function outputMemoryUsage(): void
    {
        $this->outputLine('! Memory usage %s', [Files::bytesToSizeString(memory_get_usage(true))]);
    }
}
