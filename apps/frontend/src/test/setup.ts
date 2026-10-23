import '@testing-library/jest-dom/vitest'

// React Flow (@xyflow/react) relies on ResizeObserver, which jsdom does not
// implement. A no-op polyfill is enough for component tests (the real canvas is
// not laid out under jsdom).
class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}

if (!('ResizeObserver' in globalThis)) {
  globalThis.ResizeObserver = ResizeObserverStub as unknown as typeof ResizeObserver
}
