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

    public function indexGraphNode(Node $dataNode, DimensionSpacePoint $dimensionSpacePoint, string $dimensionHash): void
    {
        $mappingType = $this->getIndex()->findType($dataNode->getNodeType()->getName());
        $matchingSubgraph = $this->contentGraph->getSubgraphByIdentifier(
            ContentStreamIdentifier::fromString($dimensionSpacePoint->getCoordinate(
                new ContentDimensionIdentifier(LegacyConfigurationAndWorkspaceBasedContentDimensionSource::WORKSPACE_DIMENSION_IDENTIFIER)
            )),
            $dimensionSpacePoint
        );
        if (!$matchingSubgraph) {
            return;
        }
        $virtualVariant = new TraversableNode($dataNode, $matchingSubgraph);
        $nodeAdapter = new LegacyNodeAdapter($virtualVariant);
        $fulltextIndexOfNode = [];
        $nodePropertiesToBeStoredInIndex = $this->extractPropertiesAndFulltext($nodeAdapter, $fulltextIndexOfNode);

        $document = new ElasticSearchDocument(
            $mappingType,
            $nodePropertiesToBeStoredInIndex,
            sha1($nodeAdapter->getContextPath())
        );
        $documentData = $document->getData();
        $documentData['__sortIndex'] = [];
        $documentData['__hierarchyRelations'] = [];

        foreach ($dataNode->getIncomingHierarchyRelations() as $incomingEdge) {
            $incomingDimensionSpacePointWithoutWorkspace = $this->removeWorkspaceFromDimensionSpacePoint($incomingEdge->getSubgraph()->getDimensionSpacePoint());
            if ($incomingDimensionSpacePointWithoutWorkspace->equals($dimensionSpacePoint)) {
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

        $this->currentBulkRequest[] = new BulkRequestPart($dimensionHash, $this->indexerDriver->document($this->getIndexName(), $nodeAdapter, $document, $documentData));
        if ($this->isFulltextEnabled($nodeAdapter)) {
            $this->currentBulkRequest[] = new BulkRequestPart($dimensionHash, $this->indexerDriver->fulltext($nodeAdapter, $fulltextIndexOfNode));
        }

        $this->flushIfNeeded();

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
    private function removeWorkspaceFromDimensionSpacePoint(DimensionSpacePoint $dimensionSpacePoint): DimensionSpacePoint
    {
        $coordinates = $dimensionSpacePoint->getCoordinates();
        unset($coordinates[LegacyConfigurationAndWorkspaceBasedContentDimensionSource::WORKSPACE_DIMENSION_IDENTIFIER]);

        return new DimensionSpacePoint($coordinates);
    }

    protected function reset(): void
    {
        $this->currentBulkRequest = [];
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
