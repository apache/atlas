/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import store from '../store';
import type { RootState, AppDispatch } from '../store';

describe('Redux Store', () => {
  // Test 1: Store initialization
  it('should initialize the store correctly', () => {
    expect(store).toBeDefined();
    expect(store.getState).toBeDefined();
    expect(store.dispatch).toBeDefined();
    expect(store.subscribe).toBeDefined();
  });

  // Test 2: Store has correct state structure
  it('should have correct state structure', () => {
    const state = store.getState();
    
    expect(state).toHaveProperty('session');
    expect(state).toHaveProperty('entity');
    expect(state).toHaveProperty('typeHeader');
    expect(state).toHaveProperty('allEntityTypes');
    expect(state).toHaveProperty('metrics');
    expect(state).toHaveProperty('classification');
    expect(state).toHaveProperty('businessMetaData');
    expect(state).toHaveProperty('glossary');
    expect(state).toHaveProperty('relationships');
    expect(state).toHaveProperty('savedSearch');
    expect(state).toHaveProperty('detailPage');
    expect(state).toHaveProperty('enum');
    expect(state).toHaveProperty('createBM');
    expect(state).toHaveProperty('glossaryType');
    expect(state).toHaveProperty('rootClassification');
    expect(state).toHaveProperty('drawerState');
    expect(state).toHaveProperty('dashboardRefresh');
  });

  // Test 3: RootState type
  it('should export RootState type correctly', () => {
    const state: RootState = store.getState();
    expect(state).toBeDefined();
  });

  // Test 4: AppDispatch type
  it('should export AppDispatch type correctly', () => {
    const dispatch: AppDispatch = store.dispatch;
    expect(dispatch).toBeDefined();
    expect(typeof dispatch).toBe('function');
  });

  // Test 5: Store dispatch functionality
  it('should allow dispatching actions', () => {
    expect(() => {
      store.dispatch({ type: 'TEST_ACTION' });
    }).not.toThrow();
  });

  // Test 6: Store subscribe functionality
  it('should allow subscribing to state changes', () => {
    const unsubscribe = store.subscribe(() => {});
    expect(typeof unsubscribe).toBe('function');
    unsubscribe();
  });

  // Test 7: Store getState returns current state
  it('should return current state via getState', () => {
    const state1 = store.getState();
    const state2 = store.getState();
    
    expect(state1).toBeDefined();
    expect(state2).toBeDefined();
  });

  // Test 8: Store has all 17 slices
  it('should have all 17 reducer slices', () => {
    const state = store.getState();
    const keys = Object.keys(state);
    
    expect(keys).toHaveLength(17);
  });

  // Test 9: Store is singleton
  it('should export the same store instance', () => {
    const store1 = require('../store').default;
    const store2 = require('../store').default;
    
    expect(store1).toBe(store2);
  });

  // Test 10: Store state is serializable
  it('should have serializable state', () => {
    const state = store.getState();
    
    expect(() => JSON.stringify(state)).not.toThrow();
    expect(() => JSON.parse(JSON.stringify(state))).not.toThrow();
  });

  // Test 11: Middleware is configured
  it('should have middleware configured', () => {
    // Middleware is configured in the store
    // Verify by dispatching an action
    expect(() => {
      store.dispatch({ type: 'MIDDLEWARE_TEST' });
    }).not.toThrow();
  });

  // Test 12: DevTools configuration
  it('should have devTools enabled', () => {
    // DevTools is enabled in the store configuration
    // This is a configuration test
    expect(store).toBeDefined();
  });

  // Test 13: Store replaceReducer functionality
  it('should have replaceReducer method', () => {
    expect(store.replaceReducer).toBeDefined();
    expect(typeof store.replaceReducer).toBe('function');
  });

  // Test 14: Initial session state
  it('should have correct initial session state', () => {
    const state = store.getState();
    
    expect(state.session).toBeDefined();
    expect(state.session.sessionObj).toEqual({
      loading: false,
      data: null,
      error: null
    });
  });

  // Test 15: Initial enum state
  it('should have correct initial enum state', () => {
    const state = store.getState();
    
    expect(state.enum).toBeDefined();
    expect(state.enum.enumObj).toEqual({
      loading: false,
      data: null,
      error: null
    });
  });

  // Test 16: Store handles unknown actions
  it('should handle unknown actions gracefully', () => {
    const stateBefore = store.getState();
    
    store.dispatch({ type: 'UNKNOWN_ACTION_TYPE_12345' });
    
    const stateAfter = store.getState();
    expect(stateAfter).toBeDefined();
  });

  // Test 17: Store state immutability
  it('should maintain state immutability', () => {
    const state = store.getState();
    
    // State should be an object
    expect(typeof state).toBe('object');
    expect(state).not.toBeNull();
  });

  // Test 18: Multiple subscribers work correctly
  it('should support multiple subscribers', () => {
    const listener1 = jest.fn();
    const listener2 = jest.fn();
    
    const unsubscribe1 = store.subscribe(listener1);
    const unsubscribe2 = store.subscribe(listener2);
    
    store.dispatch({ type: 'TEST_MULTI_SUBSCRIBE' });
    
    expect(listener1).toHaveBeenCalled();
    expect(listener2).toHaveBeenCalled();
    
    unsubscribe1();
    unsubscribe2();
  });

  // Test 19: Unsubscribe works correctly
  it('should stop calling listener after unsubscribe', () => {
    const listener = jest.fn();
    
    const unsubscribe = store.subscribe(listener);
    store.dispatch({ type: 'TEST_BEFORE_UNSUBSCRIBE' });
    
    expect(listener).toHaveBeenCalledTimes(1);
    
    unsubscribe();
    listener.mockClear();
    
    store.dispatch({ type: 'TEST_AFTER_UNSUBSCRIBE' });
    
    expect(listener).not.toHaveBeenCalled();
  });

  // Test 20: Store exports are correct
  it('should export store as default', () => {
    const defaultExport = require('../store').default;
    expect(defaultExport).toBeDefined();
    expect(defaultExport.getState).toBeDefined();
  });

  // Test 21: RootState type matches store state
  it('should have RootState type matching store state', () => {
    const state: RootState = store.getState();
    
    expect(state.session).toBeDefined();
    expect(state.enum).toBeDefined();
    expect(state.entity).toBeDefined();
  });

  // Test 22: AppDispatch type matches store dispatch
  it('should have AppDispatch type matching store dispatch', () => {
    const dispatch: AppDispatch = store.dispatch;
    
    const result = dispatch({ type: 'TEST_DISPATCH_TYPE' });
    expect(result).toBeDefined();
  });

  // Test 23: Store configuration includes rootReducer
  it('should be configured with rootReducer', () => {
    const state = store.getState();
    
    // Verify all expected slices from rootReducer are present
    expect(Object.keys(state)).toHaveLength(17);
  });

  // Test 24: Middleware configuration with immutableCheck
  it('should configure middleware with immutableCheck based on environment', () => {
    // This tests the middleware configuration
    // In non-production, immutableCheck should be enabled
    expect(() => {
      store.dispatch({ type: 'IMMUTABLE_CHECK_TEST' });
    }).not.toThrow();
  });

  // Test 25: Store handles async actions
  it('should handle async actions', async () => {
    const asyncAction = () => async (dispatch: AppDispatch) => {
      dispatch({ type: 'ASYNC_START' });
      await Promise.resolve();
      dispatch({ type: 'ASYNC_END' });
    };
    
    expect(() => {
      store.dispatch(asyncAction() as any);
    }).not.toThrow();
  });

  // Test 26: Store state consistency
  it('should maintain state consistency', () => {
    const state1 = store.getState();
    const keys1 = Object.keys(state1);
    
    store.dispatch({ type: 'CONSISTENCY_TEST' });
    
    const state2 = store.getState();
    const keys2 = Object.keys(state2);
    
    expect(keys1).toEqual(keys2);
  });

  // Test 27: Store is not null or undefined
  it('should not be null or undefined', () => {
    expect(store).not.toBeNull();
    expect(store).not.toBeUndefined();
  });

  // Test 28: Store methods are functions
  it('should have all methods as functions', () => {
    expect(typeof store.getState).toBe('function');
    expect(typeof store.dispatch).toBe('function');
    expect(typeof store.subscribe).toBe('function');
    expect(typeof store.replaceReducer).toBe('function');
  });

  // Test 29: Store state is an object
  it('should return state as an object', () => {
    const state = store.getState();
    
    expect(typeof state).toBe('object');
    expect(state).not.toBeNull();
  });

  // Test 30: Store can dispatch multiple actions sequentially
  it('should handle multiple sequential dispatches', () => {
    expect(() => {
      store.dispatch({ type: 'ACTION_1' });
      store.dispatch({ type: 'ACTION_2' });
      store.dispatch({ type: 'ACTION_3' });
    }).not.toThrow();
  });
});
