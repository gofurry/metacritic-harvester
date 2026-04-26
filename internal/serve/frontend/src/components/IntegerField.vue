<script setup>
defineOptions({ inheritAttrs: false });

const props = defineProps({
  modelValue: {
    type: [String, Number],
    default: ""
  },
  min: {
    type: Number,
    default: 0
  }
});

const emit = defineEmits(["update:modelValue"]);

function onInput(event) {
  const raw = event.target.value ?? "";
  const cleaned = String(raw).replace(/\D+/g, "");
  event.target.value = cleaned;

  if (cleaned === "") {
    emit("update:modelValue", "");
    return;
  }

  const parsed = Number(cleaned);
  emit("update:modelValue", parsed < props.min ? props.min : parsed);
}
</script>

<template>
  <input
    :value="modelValue"
    type="text"
    inputmode="numeric"
    pattern="[0-9]*"
    v-bind="$attrs"
    @input="onInput"
  />
</template>
