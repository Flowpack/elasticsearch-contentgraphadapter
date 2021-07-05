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

use Neos\Flow\Annotations as Flow;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Eel\ElasticSearchQueryBuilder as BaseElasticSearchQueryBuilder;

/**
 * Query Builder for ElasticSearch Queries
 */
class ElasticSearchQueryBuilder extends BaseElasticSearchQueryBuilder
{

    /**
     * @var int
     * @Flow\InjectConfiguration(path="driver.version", package="Flowpack.ElasticSearch.ContentRepositoryAdaptor")
     */
    protected $driverVersion;

    /**
     * @var array
     * @Flow\InjectConfiguration(path="driver.mapping", package="Flowpack.ElasticSearch.ContentRepositoryAdaptor")
     */
    protected $mapping;

    public function initializeObject() {
        // we initialize an adjusted query here
        $this->request = new ElasticSearchQuery(...array_values($this->mapping[$this->driverVersion]['query']['arguments']));
    }
}
