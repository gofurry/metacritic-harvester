<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from "vue";

const props = defineProps({
  modelValue: {
    type: [String, Number],
    default: ""
  },
  options: {
    type: Array,
    default: () => []
  }
});

const emit = defineEmits(["update:modelValue"]);

const root = ref(null);
const open = ref(false);

const selectedLabel = computed(() => {
  const match = props.options.find((option) => option.value === props.modelValue);
  return match?.label ?? "";
});

function selectValue(value) {
  emit("update:modelValue", value);
  open.value = false;
}

function toggleOpen() {
  open.value = !open.value;
}

function handleWindowClick(event) {
  if (!root.value?.contains(event.target)) {
    open.value = false;
  }
}

onMounted(() => {
  window.addEventListener("click", handleWindowClick);
});

onBeforeUnmount(() => {
  window.removeEventListener("click", handleWindowClick);
});
</script>

<template>
  <div ref="root" class="relative">
    <button
      type="button"
      class="flex w-full items-center justify-between border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
      @click.stop="toggleOpen"
    >
      <span class="truncate">{{ selectedLabel }}</span>
      <span class="ml-3 text-xs text-muted">{{ open ? "▴" : "▾" }}</span>
    </button>

    <div
      v-if="open"
      class="absolute left-0 right-0 top-[calc(100%+4px)] z-30 border border-line bg-panel shadow-[0_18px_38px_rgba(0,0,0,0.32)]"
    >
      <button
        v-for="option in options"
        :key="option.value"
        type="button"
        class="flex w-full items-center justify-between border-b border-white/4 px-3 py-2 text-left text-sm text-text transition last:border-b-0 hover:bg-accent-soft/70"
        @click.stop="selectValue(option.value)"
      >
        <span>{{ option.label }}</span>
        <span v-if="option.value === modelValue" class="text-accent">●</span>
      </button>
    </div>
  </div>
</template>
