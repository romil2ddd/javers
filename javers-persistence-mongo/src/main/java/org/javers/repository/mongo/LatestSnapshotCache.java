package org.javers.repository.mongo;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.List;
import java.util.Optional;
import org.javers.core.metamodel.object.CdoSnapshot;
import org.javers.core.metamodel.object.GlobalId;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author bartosz.walacik
 */
class LatestSnapshotCache {
    private final Cache<GlobalId, Optional<CdoSnapshot>> cache;
    private final Function<GlobalId, Optional<CdoSnapshot>> source;
    private final Function<Set<GlobalId>, List<CdoSnapshot>> fetchSnapshots;
    private final boolean disabled;

    LatestSnapshotCache(int size,
                        Function<GlobalId, Optional<CdoSnapshot>> source,
                        Function<Set<GlobalId>, List<CdoSnapshot>> fetchSnapshots) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(size)
                .build();

        this.source = source;
        this.fetchSnapshots = fetchSnapshots;
        this.disabled = size == 0;
    }

    Optional<CdoSnapshot> getLatest(GlobalId globalId) {
        if (disabled) {
            return source.apply(globalId);
        }

        Optional<CdoSnapshot> fromCache = cache.getIfPresent(globalId);

        if (fromCache != null) {
            return fromCache;
        }

        Optional<CdoSnapshot> fromDb = source.apply(globalId);
        cache.put(globalId, fromDb);
        return fromDb;
    }

    List<CdoSnapshot> getLatest(final Set<GlobalId> globalIds) {
        if (disabled) {
            return fetchSnapshots.apply(globalIds);
        }

        final List<CdoSnapshot> fromCache = globalIds.stream()
            .map(cache::getIfPresent)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        if (fromCache != null) {
            return fromCache;
        }

        final List<CdoSnapshot> fromDb = fetchSnapshots.apply(globalIds);
        fromCache.forEach(this::put);
        return fromDb;
    }

    void put(CdoSnapshot cdoSnapshot) {
        if (disabled) {
            return;
        }
        cache.put(cdoSnapshot.getGlobalId(), Optional.of(cdoSnapshot));
    }
}
