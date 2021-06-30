<?php

namespace Flowpack\ElasticSearch\ContentGraphAdapter\Mapping;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentGraphAdapter package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */
use Flowpack\ElasticSearch\Domain\Model\Index;
use Flowpack\ElasticSearch\Domain\Model\Mapping;
use Flowpack\ElasticSearch\Mapping\MappingCollection;
use Neos\Error\Messages\Warning;
use Neos\Flow\Annotations as Flow;
use Neos\ContentRepository\Domain\Model\NodeType;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\Version6\Mapping as FlowpackMapping;

/**
 * Builds the mapping information for Content Repository Node Types in Elastic Search
 *
 * @Flow\Scope("singleton")
 */
class NodeTypeMappingBuilder extends FlowpackMapping\NodeTypeMappingBuilder
{
    /**
     * Builds a Mapping Collection from the configured node types
     *
     * @param \Flowpack\ElasticSearch\Domain\Model\Index $index
     * @return \Flowpack\ElasticSearch\Mapping\MappingCollection<\Flowpack\ElasticSearch\Domain\Model\Mapping>
     */
    public function buildMappingInformation(Index $index): MappingCollection
    {
        $this->lastMappingErrors = new \Neos\Error\Messages\Result();

        $mappings = new MappingCollection(MappingCollection::TYPE_ENTITY);

        /** @var NodeType $nodeType */
        foreach ($this->nodeTypeManager->getNodeTypes() as $nodeTypeName => $nodeType) {
            if ($nodeTypeName === 'unstructured' || $nodeType->isAbstract()) {
                continue;
            }

            if ($this->nodeTypeIndexingConfiguration->isIndexable($nodeType) === false) {
                continue;
            }

            $mapping = new Mapping($index->findType($nodeTypeName));
            $fullConfiguration = $nodeType->getFullConfiguration();
            if (isset($fullConfiguration['search']['elasticSearchMapping'])) {
                $mapping->setFullMapping($fullConfiguration['search']['elasticSearchMapping']);
            }

            $mapping->setPropertyByPath('__hierarchyRelations', [
                'type' => 'object',
                'properties' => [
                    'subgraph' => [
                        'type' => 'keyword'
                    ],
                    'sortIndex' => [
                        'type' => 'integer'
                    ],
                    'accessRoles' => [
                        'type' => 'keyword'
                    ],
                    'hidden' => [
                        'type' => 'boolean'
                    ],
                    'hiddenBeforeDateTime' => [
                        'type' => 'date',
                        'format' => 'date_time_no_millis'
                    ],
                    'hiddenAfterDateTime' => [
                        'type' => 'date',
                        'format' => 'date_time_no_millis'
                    ],
                    'hiddenInIndex' => [
                        'type' => 'boolean'
                    ],
                ]
            ]);

            $mapping->setPropertyByPath('__incomingReferenceEdges', [
                'type' => 'object',
                'properties' => [
                    'source' => [
                        'type' => 'keyword'
                    ],
                    'name' => [
                        'type' => 'keyword'
                    ]
                ]
            ]);

            $mapping->setPropertyByPath('__outgoingReferenceEdges', [
                'type' => 'object',
                'properties' => [
                    'target' => [
                        'type' => 'keyword'
                    ],
                    'name' => [
                        'type' => 'keyword'
                    ],
                    'sortIndex' => [
                        'type' => 'integer'
                    ]
                ]
            ]);

            foreach ($nodeType->getProperties() as $propertyName => $propertyConfiguration) {
                // This property is configured to not be index, so do not add a mapping for it
                if (isset($propertyConfiguration['search']) && array_key_exists('indexing', $propertyConfiguration['search']) && $propertyConfiguration['search']['indexing'] === false) {
                    continue;
                }

                if (isset($propertyConfiguration['search']['elasticSearchMapping'])) {
                    if (is_array($propertyConfiguration['search']['elasticSearchMapping'])) {
                        $propertyMapping = array_filter($propertyConfiguration['search']['elasticSearchMapping'], static function ($value) {
                            return $value !== null;
                        });
                        $mapping->setPropertyByPath($propertyName, $propertyMapping);
                    }
                } elseif (isset($propertyConfiguration['type'], $this->defaultConfigurationPerType[$propertyConfiguration['type']]['elasticSearchMapping'])) {
                    if (is_array($this->defaultConfigurationPerType[$propertyConfiguration['type']]['elasticSearchMapping'])) {
                        $mapping->setPropertyByPath($propertyName, $this->defaultConfigurationPerType[$propertyConfiguration['type']]['elasticSearchMapping']);
                    }
                } else {
                    $this->lastMappingErrors->addWarning(new Warning('Node Type "' . $nodeTypeName . '" - property "' . $propertyName . '": No ElasticSearch Mapping found.'));
                }
            }

            $mappings->add($mapping);
        }

        return $mappings;
    }
}
