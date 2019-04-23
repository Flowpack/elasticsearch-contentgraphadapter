<?php

declare(strict_types=1);

namespace Flowpack\ElasticSearch\ContentGraphAdapter\NodeAggregate;

/*
 * This file is part of the Neos.ContentRepository.InMemoryGraph package.
 */

use Neos\ContentRepository\DimensionSpace\Dimension\ContentDimensionIdentifier;
use Neos\ContentRepository\Domain as ContentRepository;
use Neos\ContentRepository\Domain\ContentSubgraph\NodePath;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\ContentRepository\Domain\Model\NodeTemplate;
use Neos\ContentRepository\Domain\Model\NodeType;
use Neos\ContentRepository\Domain\Model\Workspace;
use Neos\ContentRepository\Domain\NodeAggregate\NodeName;
use Neos\ContentRepository\Domain\NodeType\NodeTypeConstraints;
use Neos\ContentRepository\Domain\Projection\Content\PropertyCollectionInterface;
use Neos\ContentRepository\Domain\Utility\NodePaths;
use Neos\ContentRepository\InMemoryGraph\ContentSubgraph\TraversableNode;
use Neos\ContentRepository\InMemoryGraph\Dimension\LegacyConfigurationAndWorkspaceBasedContentDimensionSource;
use Neos\Flow\Annotations as Flow;

/**
 * The traversable read only node implementation
 */
final class LegacyNodeAdapter implements ContentRepository\Model\NodeInterface
{
    /**
     * @Flow\Inject
     * @var ContentRepository\NodeType\NodeTypeConstraintFactory
     */
    protected $nodeTypeConstraintFactory;

    /**
     * @var TraversableNode
     */
    protected $node;

    public function __construct(TraversableNode $node)
    {
        $this->node = $node;
    }

    public function setName($newName): void
    {
        $this->handleLegacyOperation();
    }

    public function getName(): string
    {
        return (string)$this->node->getNodeName();
    }

    public function getLabel(): string
    {
        return $this->node->getLabel();
    }

    public function setProperty($propertyName, $value): void
    {
        $this->handleLegacyOperation();
    }

    public function hasProperty($propertyName): bool
    {
        return $this->node->hasProperty($propertyName);
    }

    public function getProperty($propertyName)
    {
        return $this->node->getProperty($propertyName);
    }

    public function removeProperty($propertyName): void
    {
        $this->handleLegacyOperation();
    }

    public function getProperties(): PropertyCollectionInterface
    {
        return $this->node->getProperties();
    }

    public function getPropertyNames(): void
    {
        $this->handleLegacyOperation();
    }

    public function setContentObject($contentObject): void
    {
        $this->handleLegacyOperation();
    }

    public function getContentObject(): void
    {
        $this->handleLegacyOperation();
    }

    public function unsetContentObject(): void
    {
        $this->handleLegacyOperation();
    }

    public function setNodeType(NodeType $nodeType): void
    {
        $this->handleLegacyOperation();
    }

    public function getNodeType(): NodeType
    {
        return $this->node->getNodeType();
    }

    public function setHidden($hidden): void
    {
        $this->handleLegacyOperation();
    }

    public function isHidden(): bool
    {
        return $this->node->isHidden();
    }

    public function setHiddenBeforeDateTime(\DateTimeInterface $dateTime = null): void
    {
        $this->handleLegacyOperation();
    }

    public function getHiddenBeforeDateTime(): ?\DateTimeInterface
    {
        return $this->node->getHiddenBeforeDateTime();
    }

    public function setHiddenAfterDateTime(\DateTimeInterface $dateTime = null)
    {
        $this->handleLegacyOperation();
    }

    public function getHiddenAfterDateTime(): ?\DateTimeInterface
    {
        return $this->node->getHiddenAfterDateTime();
    }

    public function setHiddenInIndex($hidden): void
    {
        $this->handleLegacyOperation();
    }

    public function isHiddenInIndex(): bool
    {
        return $this->node->isHiddenInIndex();
    }

    public function setAccessRoles(array $accessRoles): void
    {
        $this->handleLegacyOperation();
    }

    public function getAccessRoles(): array
    {
        return $this->node->getAccessRoles();
    }

    public function getPath(): string
    {
        return (string)$this->node->findNodePath();
    }

    public function getContextPath(): string
    {
        return NodePaths::generateContextPath(
            $this->getPath(),
            $this->node->getDimensionSpacePoint()->getCoordinate(new ContentDimensionIdentifier(LegacyConfigurationAndWorkspaceBasedContentDimensionSource::WORKSPACE_DIMENSION_IDENTIFIER)),
            $this->node->getDimensionSpacePoint()->toLegacyDimensionArray()
        );
    }

    public function getDepth(): int
    {
        return $this->node->getDepth();
    }

    public function setWorkspace(Workspace $workspace): void
    {
        $this->handleLegacyOperation();
    }

    public function getWorkspace(): ?Workspace
    {
        return $this->node->getWorkspace();
    }

    public function getIdentifier(): string
    {
        return (string)$this->node->getNodeAggregateIdentifier();
    }

    public function setIndex($index): void
    {
        $this->handleLegacyOperation();
    }

    public function getIndex(): int
    {
        return $this->node->getIndex();
    }

    public function getParent(): NodeInterface
    {
        return new LegacyNodeAdapter($this->node->findParentNode());
    }

    public function getParentPath()
    {
        return $this->node->getParentPath();
    }

    public function createNode($name, NodeType $nodeType = null, $identifier = null)
    {
        $this->handleLegacyOperation();
    }

    public function createSingleNode($name, NodeType $nodeType = null, $identifier = null)
    {
        $this->handleLegacyOperation();
    }

    public function createNodeFromTemplate(NodeTemplate $nodeTemplate, $nodeName = null)
    {
        $this->handleLegacyOperation();
    }

    public function getNode($path): NodeInterface
    {
        $child = $this->node;
        foreach (NodePath::fromString($path) as $nodeName) {
            $child = $child ? $child->findNamedChildNode(NodeName::fromString($nodeName)) : null;
        }

        return $child ? new LegacyNodeAdapter($child) : null;
    }

    public function getPrimaryChildNode(): NodeInterface
    {
        $childNodes = $this->node->findChildNodes();

        return new LegacyNodeAdapter(reset($childNodes));
    }

    /**
     * @param string $nodeTypeFilter
     * @param int $limit
     * @param int $offset
     * @return array|LegacyNodeAdapter[]
     */
    public function getChildNodes($nodeTypeFilter = null, $limit = null, $offset = null)
    {
        $childNodes = [];
        foreach ($this->node->findChildNodes(is_null($nodeTypeFilter) ? new NodeTypeConstraints(true) : $this->nodeTypeConstraintFactory->parseFilterString($nodeTypeFilter), $limit, $offset) as $traversableNode) {
            $childNodes[] = new LegacyNodeAdapter($traversableNode);
        };

        return $childNodes;
    }

    public function hasChildNodes($nodeTypeFilter = null): bool
    {
        return count($this->getChildNodes($nodeTypeFilter)) > 0;
    }

    public function remove()
    {
        $this->handleLegacyOperation();
    }

    public function setRemoved($removed)
    {
        $this->handleLegacyOperation();
    }

    public function isRemoved()
    {
        return $this->node->isRemoved();
    }

    public function isVisible()
    {
        return $this->node->isVisible();
    }

    public function isAccessible()
    {
        return $this->node->isAccessible();
    }

    public function hasAccessRestrictions()
    {
        return $this->node->hasAccessRestrictions();
    }

    public function isNodeTypeAllowedAsChildNode(NodeType $nodeType)
    {
        if ($this->node->isTethered()) {
            return $this->getParent()->getNodeType()->allowsGrandchildNodeType($this->getName(), $nodeType);
        } else {
            return $this->getNodeType()->allowsChildNodeType($nodeType);
        }
    }

    public function moveBefore(NodeInterface $referenceNode)
    {
        $this->handleLegacyOperation();
    }

    public function moveAfter(NodeInterface $referenceNode)
    {
        $this->handleLegacyOperation();
    }

    public function moveInto(NodeInterface $referenceNode)
    {
        $this->handleLegacyOperation();
    }

    public function copyBefore(NodeInterface $referenceNode, $nodeName)
    {
        $this->handleLegacyOperation();
    }

    public function copyAfter(NodeInterface $referenceNode, $nodeName)
    {
        $this->handleLegacyOperation();
    }

    public function copyInto(NodeInterface $referenceNode, $nodeName)
    {
        $this->handleLegacyOperation();
    }

    public function getNodeData()
    {
        $this->handleLegacyOperation();
    }

    public function getContext()
    {
        return $this->node->getContext();
    }

    public function getDimensions()
    {
        return $this->node->getDimensions();
    }

    public function createVariantForContext($context)
    {
        $this->handleLegacyOperation();
    }

    public function isAutoCreated()
    {
        return $this->node->isTethered();
    }

    public function getOtherNodeVariants()
    {
        $this->handleLegacyOperation();
    }

    /**
     * @throws LegacyOperationIsUnsupported
     */
    private function handleLegacyOperation(): void
    {
        throw new LegacyOperationIsUnsupported('Legacy operation "' . __METHOD__ . ' is not supported', 1556029877);
    }
}
