<?php
namespace Flowpack\ElasticSearch\ContentGraphAdapter\Eel;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentGraphAdapter package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Eel\ElasticSearchQuery;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Security\Context as SecurityContext;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\ContentRepository\Domain\Service\ContentDimensionPresetSourceInterface;
use Neos\ContentRepository\Search\Search\QueryBuilderInterface;
use Neos\ContentRepository\DimensionSpace\DimensionSpace;
use Neos\ContentRepository\InMemoryGraph\Dimension\DimensionSpacePointFactory;
use Neos\ContentRepository\InMemoryGraph\ContentSubgraph\ContentSubgraphIdentifier;

/**
 * Query Builder for ElasticSearch Queries
 */
class ElasticSearchQueryBuilder extends ElasticSearchQuery
{
    /**
     * @Flow\Inject
     * @var ContentDimensionPresetSourceInterface
     */
    protected $dimensionPresetSource;

    /**
     * @Flow\Inject
     * @var SecurityContext
     */
    protected $securityContext;

    /**
     * @Flow\Inject
     * @var DimensionSpacePointFactory
     */
    protected $dimensionSpacePointFactory;

    /**
     * These fields are not accepted in a count request and must therefore be removed before doing so
     *
     * @var array
     */
    protected $unsupportedFieldsInCountRequest = ['fields', 'sort', 'from', 'size', 'highlight', 'aggs', 'aggregations'];

    /**
     * Amount of total items in response without limit
     *
     * @var integer
     */
    protected $totalItems;

    /**
     * Sets the starting point for this query. Search result should only contain nodes that
     * match the context of the given node and have it as parent node in their rootline.
     *
     * @param NodeInterface $contextNode
     * @return QueryBuilderInterface
     * @api
     */
    public function query(NodeInterface $contextNode)
    {
        // on indexing, the __parentPath is tokenized to contain ALL parent path parts,
        // e.g. /foo, /foo/bar/, /foo/bar/baz; to speed up matching.. That's why we use a simple "term" filter here.
        // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-term-filter.html
        $this->queryFilter('term', ['__parentPath' => $contextNode->getPath()]);

        $workspaceName = $contextNode->getContext()->getWorkspace()->getName();

        $coordinates = [];
        foreach ($contextNode->getNodeData()->getDimensionValues() as $dimensionName => $rawDimensionValues) {
            $coordinates[$dimensionName] = reset($rawDimensionValues);
        }
        $coordinates['_workspace'] = $contextNode->getContext()->getWorkspace()->getName();

        $dimensionSpacePoint = new DimensionSpace\DimensionSpacePoint($coordinates);
        $contentSubgraphIdentifier = new ContentSubgraphIdentifier($workspaceName, $dimensionSpacePoint);

        $edgeFilter = [
            'path' => '__hierarchyRelations',
            'query' => [
                'bool' => [
                    'must' => [
                        [
                            'match' => [
                                '__hierarchyRelations.subgraph' => (string) $contentSubgraphIdentifier,
                            ],
                        ],
                    ],
                    'should' => [],
                    'must_not' => []
                ]
            ]
        ];

        if (!$contextNode->getContext()->isInvisibleContentShown()) {
            $edgeFilter['query']['bool']['must_not'][] = [
                'match' => [
                    '__hierarchyRelations.hidden' => true,
                ],
            ];
            $edgeFilter['query']['bool']['must_not'][] = [
                'range' => [
                    '__hierarchyRelations.hiddenBeforeDateTime' => [
                        'gt' => 'now'
                    ]
                ]
            ];
            $edgeFilter['query']['bool']['must_not'][] = [
                'range' => [
                    '__hierarchyRelations.hiddenAfterDateTime' => [
                        'lt' => 'now'
                    ]
                ]
            ];
        }

        /*
         * @todo make this work
        if (!$contextNode->getContext()->isInaccessibleContentShown()) {
            $edgeFilter['query']['bool']['minimum_should_match'] = 1;
            foreach (array_keys($this->securityContext->getRoles()) as $roleName) {
                $edgeFilter['query']['bool']['should'][] = [
                    'term' => [
                        '__hierarchyRelations.accessRoles' => $roleName
                    ],
                ];
            }
        }
        */

        $this->queryFilter('nested', $edgeFilter);

        $this->contextNode = $contextNode;

        return $this;
    }
}
