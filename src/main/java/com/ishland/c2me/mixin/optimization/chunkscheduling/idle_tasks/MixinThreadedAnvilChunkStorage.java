package com.ishland.c2me.mixin.optimization.chunkscheduling.idle_tasks;

import com.ishland.c2me.common.optimization.chunkscheduling.idle_tasks.IThreadedAnvilChunkStorage;
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import net.minecraft.server.world.ChunkHolder;
import net.minecraft.server.world.ThreadedAnvilChunkStorage;
import net.minecraft.util.math.ChunkPos;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Redirect;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

@Mixin(ThreadedAnvilChunkStorage.class)
public abstract class MixinThreadedAnvilChunkStorage implements IThreadedAnvilChunkStorage {

    @Shadow @Final private Long2ObjectLinkedOpenHashMap<ChunkHolder> currentChunkHolders;

    @Shadow protected abstract boolean save(ChunkHolder chunkHolder);

    @Unique
    private static final Long2ObjectLinkedOpenHashMap<ChunkHolder> anEmptyChunkHoldersMap = new Long2ObjectLinkedOpenHashMap<>();

    @Unique
    private final ConcurrentLinkedQueue<ChunkPos> dirtyChunkPosForAutoSave = new ConcurrentLinkedQueue<>();

    @Unique
    private final ObjectLinkedOpenHashSet<ChunkHolder> dirtyChunkHoldersForAutoSave = new ObjectLinkedOpenHashSet<>();

    @Redirect(method = "unloadChunks", at = @At(value = "FIELD", target = "Lnet/minecraft/server/world/ThreadedAnvilChunkStorage;chunkHolders:Lit/unimi/dsi/fastutil/longs/Long2ObjectLinkedOpenHashMap;"))
    private Long2ObjectLinkedOpenHashMap<ChunkHolder> stopAutoSaveInUnloading(ThreadedAnvilChunkStorage instance) {
        return anEmptyChunkHoldersMap; // prevent autosave from happening in unloading stage
    }

    @Override
    public void enqueueDirtyChunkPosForAutoSave(ChunkPos chunkPos) {
        this.dirtyChunkPosForAutoSave.add(chunkPos);
    }

    @Override
    public boolean runOneChunkAutoSave() {
        {
            ChunkPos pos;
            while ((pos = this.dirtyChunkPosForAutoSave.poll()) != null) {
                final ChunkHolder holder = this.currentChunkHolders.get(pos.toLong());
                if (holder != null) this.dirtyChunkHoldersForAutoSave.add(holder);
            }
        }

        final Iterator<ChunkHolder> iterator = this.dirtyChunkHoldersForAutoSave.iterator();
        while (iterator.hasNext()) {
            final ChunkHolder chunkHolder = iterator.next();
            if (chunkHolder != null) {
                if (chunkHolder.getSavingFuture().isDone()) {
                    this.save(chunkHolder);
                    iterator.remove();
                    return true;
                }
            }
        }
        return false;
    }
}
