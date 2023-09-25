package com.ishland.c2me.threading.lighting.common.starlight;

import ca.spottedleaf.starlight.common.light.BlockStarLightEngine;
import ca.spottedleaf.starlight.common.light.SkyStarLightEngine;
import ca.spottedleaf.starlight.common.light.StarLightInterface;
import ca.spottedleaf.starlight.common.util.CoordinateUtils;
import com.ishland.c2me.base.common.GlobalExecutors;
import com.ishland.c2me.base.common.scheduler.IVanillaChunkManager;
import com.ishland.c2me.base.common.scheduler.NeighborLockingUtils;
import com.ishland.c2me.base.common.scheduler.SchedulingManager;
import com.ishland.c2me.threading.lighting.mixin.starlight.access.IStarLightInterface;
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortCollection;
import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;
import net.minecraft.server.world.ServerLightingProvider;
import net.minecraft.server.world.ServerWorld;
import net.minecraft.util.math.BlockPos;
import net.minecraft.util.math.ChunkPos;
import net.minecraft.util.math.ChunkSectionPos;
import net.minecraft.world.chunk.ChunkStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public final class StarLightQueue {

    private final Long2ObjectLinkedOpenHashMap<ChunkTaskSet> chunkTasks = new Long2ObjectLinkedOpenHashMap<>();
    private final Long2ObjectLinkedOpenHashMap<CompletableFuture<Void>> scheduledChunks = new Long2ObjectLinkedOpenHashMap<>();
    private final StarLightInterface manager;
    private final Object schedulingMutex = new Object();

    public StarLightQueue(final StarLightInterface manager) {
        this.manager = manager;
    }

    public boolean isEmpty() {
        synchronized (this.schedulingMutex) {
            return this.chunkTasks.isEmpty();
        }
    }

    public ChunkTaskSet queueBlockChange(final BlockPos pos) {
        synchronized (this.schedulingMutex) {
            final ChunkTaskSet tasks = this.chunkTasks.computeIfAbsent(ChunkPos.toLong(pos), ChunkTaskSet::new);
            tasks.changedPositions.add(pos.toImmutable());
            return tasks;
        }
    }

    public ChunkTaskSet queueSectionChange(final ChunkSectionPos pos, final boolean newEmptyValue) {
        synchronized (this.schedulingMutex) {
            ChunkTaskSet tasks = this.chunkTasks.computeIfAbsent(ChunkPos.toLong(pos.getSectionX(), pos.getSectionZ()), ChunkTaskSet::new);
            if (tasks.changedSectionSet == null) {
                tasks.changedSectionSet = new Boolean[((IStarLightInterface) (Object) this.manager).getMaxSection() - ((IStarLightInterface) (Object) this.manager).getMinSection() + 1];
            }

            tasks.changedSectionSet[pos.getY() - ((IStarLightInterface) (Object) this.manager).getMinSection()] = Boolean.valueOf(newEmptyValue);
            return tasks;
        }
    }

    public ChunkTaskSet queueChunkLighting(final ChunkPos pos, final Runnable lightTask) {
        synchronized (this.schedulingMutex) {
            final ChunkTaskSet tasks = this.chunkTasks.computeIfAbsent(pos.toLong(), ChunkTaskSet::new);
            if (tasks.lightTasks == null) {
                tasks.lightTasks = new ArrayList<>();
            }

            tasks.lightTasks.add(lightTask);
            return tasks;
        }
    }

    public ChunkTaskSet queueChunkSkylightEdgeCheck(final ChunkSectionPos pos, final ShortCollection sections) {
        synchronized (this.schedulingMutex) {
            final ChunkTaskSet tasks = this.chunkTasks.computeIfAbsent(ChunkPos.toLong(pos.getSectionX(), pos.getSectionZ()), ChunkTaskSet::new);
            ShortOpenHashSet queuedEdges = tasks.queuedEdgeChecksSky;
            if (queuedEdges == null) {
                queuedEdges = tasks.queuedEdgeChecksSky = new ShortOpenHashSet();
            }

            queuedEdges.addAll(sections);
            return tasks;
        }
    }

    public ChunkTaskSet queueChunkBlocklightEdgeCheck(final ChunkSectionPos pos, final ShortCollection sections) {
        synchronized (this.schedulingMutex) {
            final ChunkTaskSet tasks = this.chunkTasks.computeIfAbsent(ChunkPos.toLong(pos.getSectionX(), pos.getSectionZ()), ChunkTaskSet::new);
            ShortOpenHashSet queuedEdges = tasks.queuedEdgeChecksBlock;
            if (queuedEdges == null) {
                queuedEdges = tasks.queuedEdgeChecksBlock = new ShortOpenHashSet();
            }

            queuedEdges.addAll(sections);
            return tasks;
        }
    }

    public void removeChunk(final ChunkPos pos) {
        final ChunkTaskSet tasks;
        synchronized (this.schedulingMutex) {
            tasks = this.chunkTasks.remove(CoordinateUtils.getChunkKey(pos));
        }
        if (tasks != null) {
            tasks.onComplete.complete(null);
        }
    }

    public ChunkTaskSet removeFirstTask() {
        if (this.chunkTasks.isEmpty()) {
            return null;
        }
        synchronized (this.schedulingMutex) {
            if (this.chunkTasks.isEmpty()) {
                return null;
            }
            return this.chunkTasks.removeFirst();
        }
    }

    public void scheduleAll() {
        if (this.manager.getWorld() instanceof ServerWorld world) {
            final SchedulingManager schedulingManager = ((IVanillaChunkManager) world.getChunkManager().threadedAnvilChunkStorage).c2me$getSchedulingManager();
            synchronized (this.schedulingMutex) {
                final ObjectBidirectionalIterator<Long2ObjectMap.Entry<ChunkTaskSet>> iterator = this.chunkTasks.long2ObjectEntrySet().fastIterator();
                while (iterator.hasNext()) {
                    final Long2ObjectMap.Entry<ChunkTaskSet> entry = iterator.next();
                    final long pos = entry.getLongKey();
                    final CompletableFuture<Void> future = this.scheduledChunks.get(pos);
                    if (future == null || future.isDone()) {
                        final ChunkTaskSet taskSet = entry.getValue();
                        iterator.remove();
                        this.scheduledChunks.remove(pos);
                        scheduleAsync0(taskSet, schedulingManager);
                    }
                }
            }
        } else {
            ChunkTaskSet taskSet;
            SkyStarLightEngine skyStarLightEngine = null;
            BlockStarLightEngine blockStarLightEngine = null;
            try {
                //noinspection DataFlowIssue
                skyStarLightEngine = ((IStarLightInterface) (Object) this.manager).invokeGetSkyLightEngine();
                blockStarLightEngine = ((IStarLightInterface) (Object) this.manager).invokeGetBlockLightEngine();
                while ((taskSet = this.removeFirstTask()) != null) {
                    try {
                        this.handleUpdateInternal(skyStarLightEngine, blockStarLightEngine, taskSet);
                    } catch (Throwable t) {
                        taskSet.onComplete.completeExceptionally(t);
                    }
                }
            } finally {
                //noinspection ConstantValue
                if (skyStarLightEngine != null)
                    ((IStarLightInterface) (Object) this.manager).invokeReleaseSkyLightEngine(skyStarLightEngine);
                //noinspection ConstantValue
                if (blockStarLightEngine != null)
                    ((IStarLightInterface) (Object) this.manager).invokeReleaseBlockLightEngine(blockStarLightEngine);
            }
        }
    }

    private void scheduleAsync0(ChunkTaskSet taskSet, SchedulingManager schedulingManager) {
        if (this.scheduledChunks.get(taskSet.chunkPos) != null) throw new AssertionError();
        final CompletableFuture<Void> future = NeighborLockingUtils.runChunkGenWithLock(
                new ChunkPos(taskSet.chunkPos),
                ChunkStatus.FULL, // only used as a hint
                null,
                2,
                schedulingManager,
                true, // this task usually runs fast, so don't pin threads
                () -> CompletableFuture.supplyAsync(() -> {
                    SkyStarLightEngine skyStarLightEngine = null;
                    BlockStarLightEngine blockStarLightEngine = null;
                    try {
                        //noinspection DataFlowIssue
                        skyStarLightEngine = ((IStarLightInterface) (Object) this.manager).invokeGetSkyLightEngine();
                        blockStarLightEngine = ((IStarLightInterface) (Object) this.manager).invokeGetBlockLightEngine();
                        this.handleUpdateInternal(skyStarLightEngine, blockStarLightEngine, taskSet);
                    } finally {
                        //noinspection ConstantValue
                        if (skyStarLightEngine != null)
                            ((IStarLightInterface) (Object) this.manager).invokeReleaseSkyLightEngine(skyStarLightEngine);
                        //noinspection ConstantValue
                        if (blockStarLightEngine != null)
                            ((IStarLightInterface) (Object) this.manager).invokeReleaseBlockLightEngine(blockStarLightEngine);
                    }
                    return taskSet.onComplete;
                }, GlobalExecutors.executor).thenCompose(Function.identity())
        );
        this.scheduledChunks.put(taskSet.chunkPos, future);
        future.whenComplete((unused, throwable) -> {
            try {
                synchronized (this.schedulingMutex) {
                    final boolean canReschedule = this.scheduledChunks.remove(taskSet.chunkPos, future); // might be scheduled elsewhere
                    if (canReschedule) {
                        final ChunkTaskSet newTask = this.chunkTasks.remove(taskSet.chunkPos);
                        if (newTask != null) {
                            scheduleAsync0(newTask, schedulingManager);
                        }
                    }
                }
                if (this.manager.lightEngine instanceof ServerLightingProvider provider) {
                    provider.tick(); // run more tasks
                }
            } catch (Throwable t) {
                t.printStackTrace();
                throw new RuntimeException(t);
            }
        });
    }

    private void handleUpdateInternal(SkyStarLightEngine skyEngine, BlockStarLightEngine blockEngine, StarLightQueue.ChunkTaskSet task) {
        if (task.lightTasks != null) {
            for (Runnable run : task.lightTasks) {
                run.run();
            }
        }

        int chunkX = CoordinateUtils.getChunkX(task.chunkPos);
        int chunkZ = CoordinateUtils.getChunkZ(task.chunkPos);
        Set<BlockPos> positions = task.changedPositions;
        Boolean[] sectionChanges = task.changedSectionSet;
        if (skyEngine != null && (!positions.isEmpty() || sectionChanges != null)) {
            skyEngine.blocksChangedInChunk(this.manager.getLightAccess(), chunkX, chunkZ, positions, sectionChanges);
        }

        if (blockEngine != null && (!positions.isEmpty() || sectionChanges != null)) {
            blockEngine.blocksChangedInChunk(this.manager.getLightAccess(), chunkX, chunkZ, positions, sectionChanges);
        }

        if (skyEngine != null && task.queuedEdgeChecksSky != null) {
            skyEngine.checkChunkEdges(this.manager.getLightAccess(), chunkX, chunkZ, task.queuedEdgeChecksSky);
        }

        if (blockEngine != null && task.queuedEdgeChecksBlock != null) {
            blockEngine.checkChunkEdges(this.manager.getLightAccess(), chunkX, chunkZ, task.queuedEdgeChecksBlock);
        }

        task.onComplete.complete(null);
    }

    public ChunkTaskSet takeChunkTasks(long pos) {
        synchronized (this.schedulingMutex) {
            return this.chunkTasks.remove(pos);
        }
    }

    public static class ChunkTaskSet {

        public final Set<BlockPos> changedPositions = new ObjectOpenHashSet<>();
        public Boolean[] changedSectionSet;
        public ShortOpenHashSet queuedEdgeChecksSky;
        public ShortOpenHashSet queuedEdgeChecksBlock;
        public List<Runnable> lightTasks;

        public final CompletableFuture<Void> onComplete = new CompletableFuture<>();

        public final long chunkPos;

        public ChunkTaskSet(final long chunkPos) {
            this.chunkPos = chunkPos;
        }
    }
}
