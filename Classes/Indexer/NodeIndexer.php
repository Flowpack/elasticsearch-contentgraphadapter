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


use Neos\Flow\Annotations as Flow;
use Flowpack\ElasticSearch\ContentGraphAdapter\NodeAggregate\LegacyNodeAdapter;
use Flowpack\ElasticSearch\Domain\Model\Document as ElasticSearchDocument;
use Neos\ContentRepository\DimensionSpace\Dimension\ContentDimensionIdentifier;
use Neos\ContentRepository\DimensionSpace\DimensionSpace\DimensionSpacePointSet;
use Neos\ContentRepository\Domain\ContentStream\ContentStreamIdentifier;
use Neos\ContentRepository\InMemoryGraph\ContentSubgraph\ContentGraph;
use Neos\ContentRepository\InMemoryGraph\ContentSubgraph\TraversableNode;
use Neos\ContentRepository\InMemoryGraph\Dimension\LegacyConfigurationAndWorkspaceBasedContentDimensionSource;
use Neos\ContentRepository\InMemoryGraph\NodeAggregate\Node;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\NodeIndexer as BaseNodeIndexer;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\BulkRequestPart;
use Neos\ContentRepository\DimensionSpace\DimensionSpace\DimensionSpacePoint;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\Flow\Log\Utility\LogEnvironment;

/**
 * Indexer for Content Repository Nodes. Triggered from the NodeIndexingManager.
 *
 * Internally, uses a bulk request.
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexer extends BaseNodeIndexer
{

    /**
     * @var ContentGraph
     */
    protected $contentGraph;

    public function indexNode(NodeInterface $node, $targetWorkspace = null): void
    {
        // Not going to happen
    }

    public function setContentGraph(ContentGraph $contentGraph): void
    {
        $this->contentGraph = $contentGraph;
    }

    public function indexGraphNode(Node $dataNode, DimensionSpacePoint $dimensionSpacePoint): void
    {
        $occupiedDimensionSpacePoints = new DimensionSpacePointSet([$dataNode->getOriginDimensionSpacePoint()]);
        $isFulltextRoot = IsFulltextRoot::isSatisfiedBy($dataNode);
        if ($isFulltextRoot) {
            $occupiedDimensionSpacePoints = $this->collectOccupiedDimensionSpacePointsForFulltextRoot($dataNode, $occupiedDimensionSpacePoints);
        }

        $mappingType = $this->getIndex()->findType($dataNode->getNodeType()->getName());
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
                $this->logger->debug(sprintf('NodeIndexer (%s) - Property "%s" not indexed because no configuration found.', $nodeAdapter->getIdentifier(), $propertyName), LogEnvironment::fromMethodName(__METHOD__));
            });

            $document = new ElasticSearchDocument(
                $mappingType,
                $nodePropertiesToBeStoredInIndex,
                sha1($nodeAdapter->getContextPath())
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
                $this->currentBulkRequest[] = new BulkRequestPart($dimensionSpacePoint->getHash(), $this->indexerDriver->document($this->getIndexName(), $nodeAdapter, $document, $documentData));
                $this->currentBulkRequest[] = new BulkRequestPart($dimensionSpacePoint->getHash(), $this->indexerDriver->fulltext($nodeAdapter, $fulltextIndexOfNode));
            }

            $serializedVariant = json_encode([
                'nodeAggregateIdentifier' => $virtualVariant->getNodeAggregateIdentifier(),
                'contentStreamIdentifier' => $virtualVariant->getContentStreamIdentifier(),
                'dimensionSpacePoint' => $virtualVariant->getDimensionSpacePoint()
            ]);

            $this->logger->debug(
                sprintf(
                    'NodeIndexer: Added / updated node %s. ID: %s Context: %s',
                    $serializedVariant,
                    $virtualVariant->getCacheEntryIdentifier(),
                    json_encode($nodeAdapter->getContext()->getProperties())
                ),
                LogEnvironment::fromMethodName(__METHOD__)
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
}
