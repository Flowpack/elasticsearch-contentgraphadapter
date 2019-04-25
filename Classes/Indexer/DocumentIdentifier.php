<?php
namespace Flowpack\ElasticSearch\ContentGraphAdapter\Indexer;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentGraphAdapter package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Neos\ContentRepository\DimensionSpace\DimensionSpace\DimensionSpacePoint;
use Neos\ContentRepository\Domain as ContentRepository;

/**
 * The ElasticSearch document identifier value object
 */
final class DocumentIdentifier
{
    /**
     * @var ContentRepository\ContentStream\ContentStreamIdentifier
     */
    protected $contentStreamIdentifier;

    /**
     * @var ContentRepository\NodeAggregate\NodeAggregateIdentifier
     */
    protected $nodeAggregateIdentifier;

    /**
     * @var DimensionSpacePoint
     */
    protected $coveredDimensionSpacePoint;

    private function __construct(
        ContentRepository\ContentStream\ContentStreamIdentifier $contentStreamIdentifier,
        ContentRepository\NodeAggregate\NodeAggregateIdentifier $nodeAggregateIdentifier,
        DimensionSpacePoint $coveredDimensionSpacePoint
    ) {
        $this->contentStreamIdentifier = $contentStreamIdentifier;
        $this->nodeAggregateIdentifier = $nodeAggregateIdentifier;
        $this->coveredDimensionSpacePoint = $coveredDimensionSpacePoint;
    }


    public static function fromTraversableNode(ContentRepository\Projection\Content\TraversableNodeInterface $traversableNode): self
    {
        return new static(
            $traversableNode->getContentStreamIdentifier(),
            $traversableNode->getNodeAggregateIdentifier(),
            $traversableNode->getDimensionSpacePoint()
        );
    }

    public static function fromLegacyNode(ContentRepository\Model\NodeInterface $node): self
    {
        return new static(
            ContentRepository\ContentStream\ContentStreamIdentifier::fromString($node->getWorkspace()->getName()),
            ContentRepository\NodeAggregate\NodeAggregateIdentifier::fromString($node->getIdentifier()),
            DimensionSpacePoint::fromLegacyDimensionArray($node->getNodeData()->getDimensionValues())
        );
    }

    public function __toString()
    {
        return sha1(json_encode([
            'nodeAggregateIdentifier' => $this->nodeAggregateIdentifier,
            'contentStreamIdentifier' => $this->contentStreamIdentifier,
            'dimensionSpacePoint' => $this->coveredDimensionSpacePoint
        ]));
    }
}
