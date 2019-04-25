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
use Neos\ContentRepository\Domain as ContentRepository;

/**
 * The specification whether a given node is a fulltext root
 */
class IsFulltextRoot
{
    public static function isSatisfiedBy(ContentRepository\Projection\Content\NodeInterface $node): bool
    {
        if ($node->getNodeType()->hasConfiguration('search')) {
            $searchSettingsForNode = $node->getNodeType()->getConfiguration('search');
            return isset($searchSettingsForNode['fulltext']['enable']) && $searchSettingsForNode['fulltext']['enable'] === true;
        }

        return false;
    }
}
