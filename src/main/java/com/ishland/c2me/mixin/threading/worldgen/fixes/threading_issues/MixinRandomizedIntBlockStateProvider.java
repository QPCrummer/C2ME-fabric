package com.ishland.c2me.mixin.threading.worldgen.fixes.threading_issues;

import net.minecraft.state.property.IntProperty;
import net.minecraft.world.gen.stateprovider.RandomizedIntBlockStateProvider;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Opcodes;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Redirect;

@Mixin(RandomizedIntBlockStateProvider.class)
public class MixinRandomizedIntBlockStateProvider {

    @Shadow @Nullable private IntProperty property;

    @Redirect(method = "getBlockState", at = @At(value = "FIELD", target = "Lnet/minecraft/world/gen/stateprovider/RandomizedIntBlockStateProvider;property:Lnet/minecraft/state/property/IntProperty;", opcode = Opcodes.PUTFIELD))
    private void redirectGetProperty(RandomizedIntBlockStateProvider randomizedIntBlockStateProvider, IntProperty value) {
        System.err.println("Detected different property settings in RandomizedIntBlockStateProvider! Expected " + this.property + " but got " + value);
        this.property = value;
    }

}
