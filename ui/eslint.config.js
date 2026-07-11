// ESLint flat config for the GrowlerDB console. Lints TS + Svelte 5; formatting is
// delegated to Prettier (eslint-config-prettier + svelte's prettier config disable conflicting rules).
import js from '@eslint/js';
import ts from 'typescript-eslint';
import svelte from 'eslint-plugin-svelte';
import prettier from 'eslint-config-prettier';
import globals from 'globals';

export default ts.config(
  { ignores: ['dist/', 'test-results/', 'playwright-report/', 'blob-report/', '**/*.jar'] },
  js.configs.recommended,
  ...ts.configs.recommended,
  ...svelte.configs.recommended,
  prettier,
  ...svelte.configs.prettier,
  {
    languageOptions: {
      globals: { ...globals.browser, ...globals.node },
    },
  },
  {
    files: ['**/*.svelte', '**/*.svelte.ts'],
    languageOptions: {
      parserOptions: { parser: ts.parser },
    },
  },
  {
    rules: {
      // This codebase updates reactive Sets *immutably* — `const next = new Set(state); next.add(x);
      // state = next` — and reassigns `$state`, which triggers reactivity correctly. The rule only
      // flags those local temporaries (and `$derived` computations), never real in-place mutation of
      // reactive state, so it's noise here. Re-enable + audit if in-place Set mutation is introduced.
      'svelte/prefer-svelte-reactivity': 'off',
    },
  },
);
