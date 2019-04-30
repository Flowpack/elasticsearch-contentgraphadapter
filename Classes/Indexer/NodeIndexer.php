<?php
namespace Flowpack\ElasticSearch\ContentGraphAdapter\Indexer;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentGraphAdaptor package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\ElasticSearch\ContentGraphAdapter\NodeAggregate\LegacyNodeAdapter;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\ElasticSearchClient;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\Version5\Mapping\NodeTypeMappingBuilder;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\IndexerDriverInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\IndexDriverInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\RequestDriverInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\SystemDriverInterface;
use Flowpack\ElasticSearch\Domain\Model\Document as ElasticSearchDocument;
use Flowpack\ElasticSearch\Domain\Model\Index;
use Neos\ContentRepository\DimensionSpace\Dimension\ContentDimensionIdentifier;
use Neos\ContentRepository\DimensionSpace\DimensionSpace\DimensionSpacePointSet;
use Neos\ContentRepository\Domain\ContentStream\ContentStreamIdentifier;
use Neos\ContentRepository\InMemoryGraph\ContentSubgraph\ContentGraph;
use Neos\ContentRepository\InMemoryGraph\ContentSubgraph\TraversableNode;
use Neos\ContentRepository\InMemoryGraph\Dimension\LegacyConfigurationAndWorkspaceBasedContentDimensionSource;
use Neos\ContentRepository\InMemoryGraph\NodeAggregate\Node;
use Neos\ContentRepository\Search\Indexer\AbstractNodeIndexer;
use Neos\Flow\Annotations as Flow;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\ContentRepository\Domain\Service\ContentDimensionCombinator;
use Neos\ContentRepository\Domain\Service\ContextFactory;
use Neos\ContentRepository\Domain\Service\NodeTypeManager;

/**
 * Indexer for Content Repository Nodes. Triggered from the NodeIndexingManager.
 *
 * Internally, uses a bulk request.
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexer extends AbstractNodeIndexer
{
    /**
     * Optional postfix for the index, e.g. to have different indexes by timestamp.
     *
     * @var string
     */
    protected $indexNamePostfix = '';

    /**
     * @Flow\Inject
     * @var ElasticSearchClient
     */
    protected $searchClient;

    /**
     * @Flow\Inject
     * @var NodeTypeMappingBuilder
     */
    protected $nodeTypeMappingBuilder;

    /**
     * @Flow\Inject
     * @var \Neos\Flow\Persistence\PersistenceManagerInterface
     */
    protected $persistenceManager;

    /**
     * @Flow\Inject
     * @var NodeTypeManager
     */
    protected $nodeTypeManager;

    /**
     * @Flow\Inject
     * @var \Flowpack\ElasticSearch\ContentRepositoryAdaptor\LoggerInterface
     */
    protected $logger;

    /**
     * @Flow\Inject
     * @var ContentDimensionCombinator
     */
    protected $contentDimensionCombinator;

    /**
     * @Flow\Inject
     * @var ContextFactory
     */
    protected $contextFactory;

    /**
     * @Flow\Inject
     * @var IndexerDriverInterface
     */
    protected $indexerDriver;

    /**
     * @var IndexDriverInterface
     * @Flow\Inject
     */
    protected $indexDriver;

    /**
     * @var RequestDriverInterface
     * @Flow\Inject
     */
    protected $requestDriver;

    /**
     * @var SystemDriverInterface
     * @Flow\Inject
     */
    protected $systemDriver;

    /**
     * @var ContentGraph
     */
    protected $contentGraph;

    /**
     * The current ElasticSearch bulk request, in the format required by http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-bulk.html
     *
     * @var array
     */
    protected $currentBulkRequest = [];

    /**
     * @var boolean
     */
    protected $bulkProcessing = false;

    /**
     * @var array
     */
    protected $fulltextRootRegistry = [];

    /**
     * @var array
     */
    protected $fulltextRegistry = [];


    /**
     * Returns the index name to be used for indexing, with optional indexNamePostfix appended.
     *
     * @return string
     */
    public function getIndexName()
    {
        $indexName = $this->searchClient->getIndexName();
        if (strlen($this->indexNamePostfix) > 0) {
            $indexName .= '-' . $this->indexNamePostfix;
        }

        return $indexName;
    }

    /**
     * Set the postfix for the index name
     *
     * @param string $indexNamePostfix
     * @return void
     */
    public function setIndexNamePostfix($indexNamePostfix)
    {
        $this->indexNamePostfix = $indexNamePostfix;
    }

    /**
     * Return the currently active index to be used for indexing
     *
     * @return Index
     */
    public function getIndex()
    {
        $index = $this->searchClient->findIndex($this->getIndexName());
        $index->setSettingsKey($this->searchClient->getIndexName());

        return $index;
    }

    public function indexNode(NodeInterface $node, $targetWorkspace = null)
    {
        // Not going to happen
    }

    public function setContentGraph(ContentGraph $contentGraph): void
    {
        $this->contentGraph = $contentGraph;
    }

    public function indexGraphNode(Node $dataNode): void
    {
        $occupiedDimensionSpacePoints = new DimensionSpacePointSet([$dataNode->getOriginDimensionSpacePoint()]);
        $isFulltextRoot = IsFulltextRoot::isSatisfiedBy($dataNode);
        if ($isFulltextRoot) {
            $occupiedDimensionSpacePoints = $this->collectOccupiedDimensionSpacePointsForFulltextRoot($dataNode, $occupiedDimensionSpacePoints);
        }

        $mappingType = $this->getIndex()->findType($this->nodeTypeMappingBuilder->convertNodeTypeNameToMappingName($dataNode->getNodeType()));
        foreach ($occupiedDimensionSpacePoints as $occupiedDimensionSpacePoint) {
            $matchingSubgraph = $this->contentGraph->getSubgraphByIdentifier(
                ContentStreamIdentifier::fromString($occupiedDimensionSpacePoint->getCoordinate(new ContentDimensionIdentifier(LegacyConfigurationAndWorkspaceBasedContentDimensionSource::WORKSPACE_DIMENSION_IDENTIFIER))),
                $occupiedDimensionSpacePoint
            );
            if (!$matchingSubgraph) {
                continue;
            }
            $virtualVariant = new TraversableNode($dataNode, $matchingSubgraph);
            $nodeAdapter = new LegacyNodeAdapter($virtualVariant);
            $fulltextIndexOfNode = [];
            $nodePropertiesToBeStoredInIndex = $this->extractPropertiesAndFulltext($nodeAdapter, $fulltextIndexOfNode, function ($propertyName) use ($nodeAdapter) {
                $this->logger->log(sprintf('NodeIndexer (%s) - Property "%s" not indexed because no configuration found.', $nodeAdapter->getIdentifier(), $propertyName), LOG_DEBUG, null, 'ElasticSearch (CR)');
            });

            $document = new ElasticSearchDocument(
                $mappingType,
                $nodePropertiesToBeStoredInIndex,
                (string)DocumentIdentifier::fromTraversableNode($virtualVariant)
            );
            $documentData = $document->getData();
            $documentData['__sortIndex'] = [];
            $documentData['__hierarchyRelations'] = [];

            foreach ($dataNode->getIncomingHierarchyRelations() as $incomingEdge) {
                $documentData['__hierarchyRelations'][] = [
                    'subgraph' => $incomingEdge->getSubgraphHash(),
                    'sortIndex' => $incomingEdge->getPosition(),
                    'accessRoles' => $incomingEdge->getProperty('accessRoles'),
                    'hidden' => $incomingEdge->getProperty('hidden'),
                    'hiddenBeforeDateTime' => $incomingEdge->getProperty('hiddenBeforeDateTime') ? $incomingEdge->getProperty('hiddenBeforeDateTime')->format('Y-m-d\TH:i:sP') : null,
                    'hiddenAfterDateTime' => $incomingEdge->getProperty('hiddenAfterDateTime') ? $incomingEdge->getProperty('hiddenAfterDateTime')->format('Y-m-d\TH:i:sP') : null,
                    'hiddenInIndex' => $incomingEdge->getProperty('hiddenInIndex')
                ];
            }

            foreach ($dataNode->getIncomingReferenceRelations() as $referenceRelation) {
                $documentData['__incomingReferenceRelations'][] = [
                    'source' => $referenceRelation->getSource()->getNodeAggregateIdentifier(),
                    'name' => $referenceRelation->getName()
                ];
            }

            foreach ($dataNode->getOutgoingReferenceRelations() as $referenceRelation) {
                $documentData['__outgoingReferenceRelations'][] = [
                    'target' => (string)$referenceRelation->getTarget()->getIdentifier(),
                    'name' => $referenceRelation->getName(),
                    'sortIndex' => $referenceRelation->getPosition()
                ];
            }

            if ($isFulltextRoot) {
                $this->currentBulkRequest[] = $this->indexerDriver->document($this->getIndexName(), $nodeAdapter, $document, $documentData);
                $this->currentBulkRequest[] = $this->indexerDriver->fulltext($nodeAdapter, $fulltextIndexOfNode);
            }

            $serializedVariant = json_encode([
                'nodeAggregateIdentifier' => $virtualVariant->getNodeAggregateIdentifier(),
                'contentStreamIdentifier' => $virtualVariant->getContentStreamIdentifier(),
                'dimensionSpacePoint' => $virtualVariant->getDimensionSpacePoint()
            ]);

            $this->logger->log(
                sprintf(
                    'NodeIndexer: Added / updated node %s. ID: %s Context: %s',
                    $serializedVariant,
                    $virtualVariant->getCacheEntryIdentifier(),
                    json_encode($nodeAdapter->getContext()->getProperties())
                ),
                LOG_DEBUG,
                null,
                'ElasticSearch (CR)'
            );
        }
    }

    protected function collectOccupiedDimensionSpacePointsForFulltextRoot(Node $currentNode, DimensionSpacePointSet $dimensionSpacePoints): DimensionSpacePointSet
    {
        if (!$dimensionSpacePoints->contains($currentNode->getOriginDimensionSpacePoint())) {
            $dimensionSpacePoints = $dimensionSpacePoints->getUnion(new DimensionSpacePointSet([$currentNode->getOriginDimensionSpacePoint()]));
        }

        foreach ($currentNode->getOutgoingHierarchyRelations() as $outgoingHierarchyRelation) {
            $childNode = $outgoingHierarchyRelation->getChild();
            if (!IsFulltextRoot::isSatisfiedBy($childNode)) {
                $dimensionSpacePoints = $this->collectOccupiedDimensionSpacePointsForFulltextRoot($outgoingHierarchyRelation->getChild(), $dimensionSpacePoints);
            }
        }

        return $dimensionSpacePoints;
    }

    /**
     * Schedule node removal into the current bulk request.
     *
     * @param NodeInterface $node
     */
    public function removeNode(NodeInterface $node)
    {
        if ($this->settings['indexAllWorkspaces'] === false) {
            if ($node->getContext()->getWorkspaceName() !== 'live') {
                return;
            }
        }

        // TODO: handle deletion from the fulltext index as well
        $identifier = (string)DocumentIdentifier::fromLegacyNode($node);

        $this->currentBulkRequest[] = [
            [
                'delete' => [
                    '_type' => $this->nodeTypeMappingBuilder->convertNodeTypeNameToMappingName($node->getNodeType()),
                    '_id' => $identifier
                ]
            ]
        ];

        $this->logger->log(sprintf('NodeIndexer: Removed node %s from index (node actually removed). Persistence ID: %s', $node->getContextPath(), $identifier), LOG_DEBUG, null, 'ElasticSearch (CR)');
    }

    /**
     * perform the current bulk request
     *
     * @return void
     */
    public function flush()
    {
        if (count($this->currentBulkRequest) === 0) {
            return;
        }

        $content = '';
        foreach ($this->currentBulkRequest as $bulkRequestTuple) {
            if (!is_array($bulkRequestTuple)) {
                continue;
            }

            $tupleAsJson = '';
            foreach ($bulkRequestTuple as $bulkRequestItem) {
                $itemAsJson = json_encode($bulkRequestItem);
                if ($itemAsJson === false) {
                    $this->logger->log('Indexing Error: Bulk request item could not be encoded as JSON - ' . json_last_error_msg(), LOG_ERR, $bulkRequestItem);
                    continue 2;
                }
                $tupleAsJson .= $itemAsJson . chr(10);
            }
            $content .= $tupleAsJson;
            if (strlen($content) > 10000000) {
                $this->sendBulkRequest($content);
                $content = '';
            }
        }

        if ($content !== '') {
            $response = $this->requestDriver->bulk($this->getIndex(), $content);
            foreach ($response as $responseLine) {
                if (isset($response['errors']) && $response['errors'] !== false) {
                    $this->logger->log('Indexing Error: Error during bulk request - ' . $responseLine, LOG_ERR);
                }
            }
        }

        $this->currentBulkRequest = [];
    }

    /**
     * @param string $content
     * @throws \Flowpack\ElasticSearch\Exception
     */
    protected function sendBulkRequest($content)
    {
        $responseAsLines = $this->getIndex()->request('POST', '/_bulk', [], $content)->getOriginalResponse()->getContent();
        foreach (explode("\n", $responseAsLines) as $responseLine) {
            $response = json_decode($responseLine);
            if (!is_object($response) || (isset($response->errors) && $response->errors !== false)) {
                $this->logger->log('Indexing Error: ' . $responseLine, LOG_ERR);
            }
        }
    }

    /**
     * Update the index alias
     *
     * @return void
     * @throws Exception
     * @throws \Flowpack\ElasticSearch\Transfer\Exception\ApiException
     * @throws \Exception
     */
    public function updateIndexAlias()
    {
        $aliasName = $this->searchClient->getIndexName(); // The alias name is the unprefixed index name
        if ($this->getIndexName() === $aliasName) {
            throw new Exception('UpdateIndexAlias is only allowed to be called when $this->setIndexNamePostfix has been created.', 1383649061);
        }

        if (!$this->getIndex()->exists()) {
            throw new Exception('The target index for updateIndexAlias does not exist. This shall never happen.', 1383649125);
        }

        $aliasActions = [];
        try {
            $response = $this->searchClient->request('GET', '/_alias/' . $aliasName);
            if ($response->getStatusCode() !== 200) {
                throw new Exception('The alias "' . $aliasName . '" was not found with some unexpected error... (return code: ' . $response->getStatusCode() . ')', 1383650137);
            }

            $indexNames = array_keys($response->getTreatedContent());

            if ($indexNames === []) {
                // if there is an actual index with the name we want to use as alias, remove it now
                $response = $this->searchClient->request('HEAD', '/' . $aliasName);
                if ($response->getStatusCode() === 200) {
                    $response = $this->searchClient->request('DELETE', '/' . $aliasName);
                    if ($response->getStatusCode() !== 200) {
                        throw new Exception('The index "' . $aliasName . '" could not be removed to be replaced by an alias. (return code: ' . $response->getStatusCode() . ')', 1395419177);
                    }
                }
            } else {
                foreach ($indexNames as $indexName) {
                    $aliasActions[] = [
                        'remove' => [
                            'index' => $indexName,
                            'alias' => $aliasName
                        ]
                    ];
                }
            }
        } catch (\Flowpack\ElasticSearch\Transfer\Exception\ApiException $exception) {
            // in case of 404, do not throw an error...
            if ($exception->getResponse()->getStatusCode() !== 404) {
                throw $exception;
            }
        }

        $aliasActions[] = [
            'add' => [
                'index' => $this->getIndexName(),
                'alias' => $aliasName
            ]
        ];

        $this->searchClient->request('POST', '/_aliases', [], \json_encode(['actions' => $aliasActions]));
    }

    /**
     * Remove old indices which are not active anymore (remember, each bulk index creates a new index from scratch,
     * making the "old" index a stale one).
     *
     * @return array<string> a list of index names which were removed
     * @throws Exception
     */
    public function removeOldIndices()
    {
        $aliasName = $this->searchClient->getIndexName(); // The alias name is the unprefixed index name

        $currentlyLiveIndices = $this->indexDriver->indexesByAlias($aliasName);

        $indexStatus = $this->systemDriver->status();
        $allIndices = array_keys($indexStatus['indices']);

        $indicesToBeRemoved = [];

        foreach ($allIndices as $indexName) {
            if (strpos($indexName, $aliasName . '-') !== 0) {
                // filter out all indices not starting with the alias-name, as they are unrelated to our application
                continue;
            }

            if (array_search($indexName, $currentlyLiveIndices) !== false) {
                // skip the currently live index names from deletion
                continue;
            }

            $indicesToBeRemoved[] = $indexName;
        }

        array_map(function ($index) {
            $this->indexDriver->deleteIndex($index);
        }, $indicesToBeRemoved);

        return $indicesToBeRemoved;
    }

    /**
     * Perform indexing without checking about duplication document
     *
     * This is used during bulk indexing to improve performance
     *
     * @param callable $callback
     * @throws \Exception
     */
    public function withBulkProcessing(callable $callback)
    {
        $bulkProcessing = $this->bulkProcessing;
        $this->bulkProcessing = true;
        try {
            /** @noinspection PhpUndefinedMethodInspection */
            $callback->__invoke();
        } catch (\Exception $exception) {
            $this->bulkProcessing = $bulkProcessing;
            throw $exception;
        }
        $this->bulkProcessing = $bulkProcessing;
    }
}
