<?php

declare(strict_types=1);

namespace Flowpack\ElasticSearch\ContentGraphAdapter\NodeAggregate;

/*
 * This file is part of the Neos.ContentRepository.InMemoryGraph package.
 */

/**
 * The exception to be thrown if a deprecated and unsupported legacy operation was tried to be called
 */
class LegacyOperationIsUnsupported extends \DomainException
{
}
