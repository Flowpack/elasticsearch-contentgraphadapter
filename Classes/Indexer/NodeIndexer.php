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

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\ElasticSearchClient;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\Version5\Mapping\NodeTypeMappingBuilder;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\IndexerDriverInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\IndexDriverInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\RequestDriverInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\SystemDriverInterface;
use Flowpack\ElasticSearch\Domain\Model\Document as ElasticSearchDocument;
use Flowpack\ElasticSearch\Domain\Model\Index;
use Neos\ContentRepository\Search\Indexer\AbstractNodeIndexer;
use Neos\Flow\Annotations as Flow;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\ContentRepository\Domain\Service\ContentDimensionCombinator;
use Neos\ContentRepository\Domain\Service\ContextFactory;
use Neos\ContentRepository\Domain\Service\NodeTypeManager;
use Neos\ContentRepository\InMemoryGraph\ReadOnlyNode;

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

    /**
     * @param ReadOnlyNode $node
     * @throws \Neos\ContentRepository\Exception\NodeTypeNotFoundException
     */
    public function indexGraphNode(ReadOnlyNode $node)
    {
        $nodeAdaptor = $node;
        $mappingType = $this->getIndex()->findType(NodeTypeMappingBuilder::convertNodeTypeNameToMappingName($nodeAdaptor->getNodeType()));

        $fulltextIndexOfNode = [];

        $contextPath = $nodeAdaptor->getContextPath();
        $contextPathHash = sha1($contextPath);
        $identifierForGraph = $node->getNodeIdentifier();
        $nodePropertiesToBeStoredInIndex = $this->extractPropertiesAndFulltext($nodeAdaptor, $fulltextIndexOfNode, function ($propertyName) use ($identifierForGraph) {
            $this->logger->log(sprintf('NodeIndexer (%s) - Property "%s" not indexed because no configuration found.', $identifierForGraph, $propertyName), LOG_DEBUG, null, 'ElasticSearch (CR)');
        });
        $document = new ElasticSearchDocument(
            $mappingType,
            $nodePropertiesToBeStoredInIndex,
            $contextPathHash
        );

        $documentData = $document->getData();
        $documentData['__sortIndex'] = [];

        $documentData['__edges'] = [];

        foreach ($node->getIncomingEdges() as $incomingEdge) {
            $documentData['__edges'][] = [
                'tree' => $incomingEdge->getSubgraphHash(),
                'sortIndex' => $incomingEdge->getPosition(),
                'accessRoles' => $incomingEdge->getProperty('accessRoles'),
                'hidden' => $incomingEdge->getProperty('hidden'),
                'hiddenBeforeDateTime' => $incomingEdge->getProperty('hiddenBeforeDateTime') ? $incomingEdge->getProperty('hiddenBeforeDateTime')->format('Y-m-d\TH:i:sP') : null,
                'hiddenAfterDateTime' => $incomingEdge->getProperty('hiddenAfterDateTime') ? $incomingEdge->getProperty('hiddenAfterDateTime')->format('Y-m-d\TH:i:sP') : null,
                'hiddenInIndex' => $incomingEdge->getProperty('hiddenInIndex')
            ];
        }

        foreach ($node->getIncomingReferenceEdges() as $referenceEdge) {
            $documentData['__incomingReferenceEdges'][] = [
                'source' => $referenceEdge->getSource()->getIdentifier(),
                'name' => $referenceEdge->getName()
            ];
        }

        foreach ($node->getOutgoingReferenceEdges() as $referenceEdge) {
            $documentData['__outgoingReferenceEdges'][] = [
                'target' => $referenceEdge->getTarget()->getIdentifier(),
                'name' => $referenceEdge->getName(),
                'sortIndex' => $referenceEdge->getPosition()
            ];
        }

        if ($this->isFulltextEnabled($nodeAdaptor)) {
            $this->currentBulkRequest[] = $this->indexerDriver->document($this->getIndexName(), $nodeAdaptor, $document, $documentData);
            $this->currentBulkRequest[] = $this->indexerDriver->fulltext($nodeAdaptor, $fulltextIndexOfNode);
        }

        $this->logger->log(sprintf('NodeIndexer: Added / updated node %s. ID: %s Context: %s', $contextPath, $contextPathHash, json_encode($nodeAdaptor->getContext()->getProperties())), LOG_DEBUG, null,
            'ElasticSearch (CR)');
    }

    /**
     * Whether the node has fulltext indexing enabled.
     *
     * @param NodeInterface $node
     * @return bool
     */
    protected function isFulltextEnabled(NodeInterface $node)
    {
        if (!isset($this->fulltextRegistry[$node->getNodeType()->getName()])) {
            $this->fulltextRegistry[$node->getNodeType()->getName()] = false;
            $nodeType = $this->nodeTypeManager->getNodeType($node->getNodeType()->getName());
            if ($nodeType->hasConfiguration('search')) {
                $searchSettingsForNode = $nodeType->getConfiguration('search');
                if (isset($searchSettingsForNode['fulltext']['enable']) && $searchSettingsForNode['fulltext']['enable'] === true) {
                    $this->fulltextRegistry[$node->getNodeType()->getName()] = true;
                }
            }
        }

        return $this->fulltextRegistry[$node->getNodeType()->getName()];
    }

    /**
     * Schedule node removal into the current bulk request.
     *
     * @param NodeInterface $node
     * @return string
     */
    public function removeNode(NodeInterface $node)
    {
        if ($this->settings['indexAllWorkspaces'] === false) {
            if ($node->getContext()->getWorkspaceName() !== 'live') {
                return;
            }
        }

        // TODO: handle deletion from the fulltext index as well
        $identifier = sha1($node->getContextPath());

        $this->currentBulkRequest[] = [
            [
                'delete' => [
                    '_type' => NodeTypeMappingBuilder::convertNodeTypeNameToMappingName($node->getNodeType()),
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
