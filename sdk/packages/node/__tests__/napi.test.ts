import { isNativeAvailable } from "../src/index.js";

test("native availability check doesn't throw", () => {
  // Should return true or false, never throw
  expect(() => isNativeAvailable()).not.toThrow();
  expect(typeof isNativeAvailable()).toBe("boolean");
});
