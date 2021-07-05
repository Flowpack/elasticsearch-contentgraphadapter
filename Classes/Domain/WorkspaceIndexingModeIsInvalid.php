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

use Neos\Flow\Annotations as Flow;

/**
 * The exception to be thrown if an invalid workspace indexing mode was attempted to be initialized
 * @Flow\Proxy(false)
 */
final class WorkspaceIndexingModeIsInvalid extends \DomainException
{
    public static function becauseItIsNoneOfTheDefinedValues(string $attemptedValue): self
    {
        return new self('Given value "' . $attemptedValue . '" is no valid workspace indexing mode, must be one of the defined constants.', 1625245635);
    }
}

