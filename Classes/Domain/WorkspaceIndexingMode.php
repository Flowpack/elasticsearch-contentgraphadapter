<?php
namespace Flowpack\ElasticSearch\ContentGraphAdapter\Domain;

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
use Neos\ContentRepository\InMemoryGraph\Dimension\LegacyConfigurationAndWorkspaceBasedContentDimensionSource;
use Neos\ContentRepository\InMemoryGraph\NodeAggregate\Node;
use Neos\Flow\Annotations as Flow;

/**
 * The workspace indexing mode
 * @Flow\Proxy(false)
 */
final class WorkspaceIndexingMode
{
    /**
     * In this mode, only nodes in workspace live are indexed.
     * Really fast but does not work on unpublished changes.
     *
     *        ┌────────┐
     *        │ P:live │
     *        └────────┘
     *        ╱        ╲
     *       ╱          ╲
     * ┌────▼────┐    ┌─────────────┐
     * │  C:live │    │  C:user-me  │ <--- not indexed
     * └─────────┘    └─────────────┘
     */
    const MODE_ONLY_LIVE = 'onlyLive';

    /**
     * In this mode, nodes are only indexed in their origin workspace.
     * Still quite fast but does not work on unpublished changes in edge cases.
     *
     *                            ┌────────────────┐
     *                            │ P:live,user-me │ <--- visible in user-me only due to fallback
     *                            └────────────────┘      thus only indexed in live
     *                             ╱             ╲
     *                            ╱               ╲
     *                      ┌────▼────┐    ┌───────▼─────┐
     * indexed in live ---> │  C:live │    │  C:user-me  │ <--- indexed in user-me
     *                      └─────────┘    └─────────────┘
     */
    const MODE_ONLY_ORIGIN = 'onlyOrigin';

    /**
     * In this mode, nodes are indexed in all their covered workspaces.
     * Not so fast due to creation of large amounts of duplicates.
     *
     *                            ┌────────────────┐
     *                            │ P:live,user-me │ <--- indexed in both live and user-me
     *                            └────────────────┘
     *                             ╱             ╲
     *                            ╱               ╲
     *                      ┌────▼────┐    ┌───────▼─────┐
     * indexed in live ---> │  C:live │    │  C:user-me  │ <--- indexed in user-me
     *                      └─────────┘    └─────────────┘
     */
    const MODE_FULL = 'full';

    /**
     * @var string
     */
    private $value;

    private function __construct(string $value)
    {
        $this->value = $value;
    }

    public static function fromString(string $value): self
    {
        if ($value !== self::MODE_ONLY_LIVE && $value !== self::MODE_ONLY_ORIGIN && $value !== self::MODE_FULL) {
            throw WorkspaceIndexingModeIsInvalid::becauseItIsNoneOfTheDefinedValues($value);
        }

        return new self($value);
    }

    public function isNodeToBeIndexed(Node $node): bool
    {
        switch ($this->value) {
            case self::MODE_ONLY_LIVE:
                return $node->getWorkspace()->getName() === 'live';
            default:
                return true;
        }
    }

    public function getValue(): string
    {
        return $this->value;
    }
}

