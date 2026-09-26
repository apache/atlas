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

import { configureStore } from '@reduxjs/toolkit';
import rootReducer from '../reducers';

describe('rootReducer', () => {
  let store: ReturnType<typeof configureStore>;

  beforeEach(() => {
    store = configureStore({
      reducer: rootReducer
    });
  });

  // Test 1: Root reducer initialization
  it('should initialize with all slice states', () => {
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

  // Test 2: Verify all slice keys exist
  it('should have exactly 17 reducer slices', () => {
    const state = store.getState();
    const keys = Object.keys(state);
    
    expect(keys).toHaveLength(17);
    expect(keys).toEqual(
      expect.arrayContaining([
        'session',
        'entity',
        'typeHeader',
        'allEntityTypes',
        'metrics',
        'dashboardRefresh',
        'classification',
        'businessMetaData',
        'glossary',
        'relationships',
        'savedSearch',
        'detailPage',
        'enum',
        'createBM',
        'glossaryType',
        'rootClassification',
        'drawerState'
      ])
    );
  });

  // Test 3: Session slice initialization
  it('should initialize session slice correctly', () => {
    const state = store.getState();
    
    expect(state.session).toBeDefined();
    expect(state.session.sessionObj).toEqual({
      loading: false,
      data: null,
      error: null
    });
  });

  // Test 4: Enum slice initialization
  it('should initialize enum slice correctly', () => {
    const state = store.getState();
    
    expect(state.enum).toBeDefined();
    expect(state.enum.enumObj).toEqual({
      loading: false,
      data: null,
      error: null
    });
  });

  // Test 5: CreateBM slice initialization
  it('should initialize createBM slice correctly', () => {
    const state = store.getState();
    
    expect(state.createBM).toBeDefined();
  });

  // Test 6: GlossaryType slice initialization
  it('should initialize glossaryType slice correctly', () => {
    const state = store.getState();
    
    expect(state.glossaryType).toBeDefined();
  });

  // Test 7: RootClassification slice initialization
  it('should initialize rootClassification slice correctly', () => {
    const state = store.getState();
    
    expect(state.rootClassification).toBeDefined();
  });

  // Test 8: BusinessMetaData slice initialization
  it('should initialize businessMetaData slice correctly', () => {
    const state = store.getState();
    
    expect(state.businessMetaData).toBeDefined();
  });

  // Test 9: Classification slice initialization
  it('should initialize classification slice correctly', () => {
    const state = store.getState();
    
    expect(state.classification).toBeDefined();
  });

  // Test 10: Entity slice initialization
  it('should initialize entity slice correctly', () => {
    const state = store.getState();
    
    expect(state.entity).toBeDefined();
  });

  // Test 11: TypeHeader slice initialization
  it('should initialize typeHeader slice correctly', () => {
    const state = store.getState();
    
    expect(state.typeHeader).toBeDefined();
  });

  // Test 12: AllEntityTypes slice initialization
  it('should initialize allEntityTypes slice correctly', () => {
    const state = store.getState();
    
    expect(state.allEntityTypes).toBeDefined();
  });

  // Test 13: DetailPage slice initialization
  it('should initialize detailPage slice correctly', () => {
    const state = store.getState();
    
    expect(state.detailPage).toBeDefined();
  });

  // Test 14: Glossary slice initialization
  it('should initialize glossary slice correctly', () => {
    const state = store.getState();
    
    expect(state.glossary).toBeDefined();
  });

  // Test 15: Relationships slice initialization
  it('should initialize relationships slice correctly', () => {
    const state = store.getState();
    
    expect(state.relationships).toBeDefined();
  });

  // Test 16: Metrics slice initialization
  it('should initialize metrics slice correctly', () => {
    const state = store.getState();
    
    expect(state.metrics).toBeDefined();
  });

  // Test 17: SavedSearch slice initialization
  it('should initialize savedSearch slice correctly', () => {
    const state = store.getState();
    
    expect(state.savedSearch).toBeDefined();
  });

  // Test 18: DrawerState slice initialization
  it('should initialize drawerState slice correctly', () => {
    const state = store.getState();
    
    expect(state.drawerState).toBeDefined();
  });

  // Test 19: Root reducer returns valid state object
  it('should return a valid state object', () => {
    const state = store.getState();
    
    expect(state).toBeInstanceOf(Object);
    expect(state).not.toBeNull();
    expect(state).not.toBeUndefined();
  });

  // Test 20: Root reducer is a function
  it('should be a function', () => {
    expect(typeof rootReducer).toBe('function');
  });

  // Test 21: Store dispatch works with root reducer
  it('should allow dispatching actions through the store', () => {
    const initialState = store.getState();
    
    expect(() => {
      store.dispatch({ type: 'UNKNOWN_ACTION' });
    }).not.toThrow();
    
    const newState = store.getState();
    expect(newState).toBeDefined();
  });

  // Test 22: Root reducer maintains state immutability
  it('should maintain state immutability', () => {
    const state1 = store.getState();
    store.dispatch({ type: 'UNKNOWN_ACTION' });
    const state2 = store.getState();
    
    // Redux may return the same reference if state doesn't change
    // Verify state structure is maintained
    expect(state2).toBeDefined();
    expect(Object.keys(state1)).toEqual(Object.keys(state2));
    expect(state2).toEqual(state1);
  });

  // Test 23: Each slice is independently manageable
  it('should have independent slice states', () => {
    const state = store.getState();
    
    // Each slice should be a separate object
    expect(state.session).not.toBe(state.enum);
    expect(state.entity).not.toBe(state.classification);
    expect(state.glossary).not.toBe(state.metrics);
  });

  // Test 24: Root reducer handles undefined state
  it('should handle undefined state gracefully', () => {
    const newState = rootReducer(undefined, { type: '@@INIT' });
    
    expect(newState).toBeDefined();
    expect(newState).toHaveProperty('session');
    expect(newState).toHaveProperty('enum');
    expect(newState).toHaveProperty('entity');
  });

  // Test 25: Root reducer returns state on action dispatch
  it('should return state on action dispatch', () => {
    const state1 = rootReducer(undefined, { type: '@@INIT' });
    const state2 = rootReducer(state1, { type: 'DUMMY_ACTION' });
    
    // Redux may return the same reference if state doesn't change
    // Verify state structure is maintained
    expect(state2).toBeDefined();
    expect(Object.keys(state1)).toEqual(Object.keys(state2));
    expect(state2).toEqual(state1);
  });

  // Test 26: Combined reducers structure is correct
  it('should have combined reducers with correct structure', () => {
    const state = store.getState();
    const expectedKeys = [
      'session',
      'entity',
      'typeHeader',
      'allEntityTypes',
      'metrics',
      'dashboardRefresh',
      'classification',
      'businessMetaData',
      'glossary',
      'relationships',
      'savedSearch',
      'detailPage',
      'enum',
      'createBM',
      'glossaryType',
      'rootClassification',
      'drawerState'
    ];
    
    expectedKeys.forEach(key => {
      expect(state).toHaveProperty(key);
    });
  });

  // Test 27: No unexpected properties in root state
  it('should not have unexpected properties in root state', () => {
    const state = store.getState();
    const keys = Object.keys(state);
    const expectedKeys = [
      'session',
      'entity',
      'typeHeader',
      'allEntityTypes',
      'metrics',
      'dashboardRefresh',
      'classification',
      'businessMetaData',
      'glossary',
      'relationships',
      'savedSearch',
      'detailPage',
      'enum',
      'createBM',
      'glossaryType',
      'rootClassification',
      'drawerState'
    ];
    
    keys.forEach(key => {
      expect(expectedKeys).toContain(key);
    });
  });

  // Test 28: Root reducer preserves slice state on unknown action
  it('should preserve slice state on unknown action', () => {
    const initialState = store.getState();
    const initialSessionState = initialState.session;
    
    store.dispatch({ type: 'UNKNOWN_ACTION_TYPE' });
    
    const newState = store.getState();
    expect(newState.session).toEqual(initialSessionState);
  });

  // Test 29: Store can be recreated with root reducer
  it('should allow creating multiple stores with root reducer', () => {
    const store1 = configureStore({ reducer: rootReducer });
    const store2 = configureStore({ reducer: rootReducer });
    
    expect(store1.getState()).toEqual(store2.getState());
    expect(store1).not.toBe(store2);
  });

  // Test 30: Root reducer is exported correctly
  it('should export root reducer as default', () => {
    expect(rootReducer).toBeDefined();
    expect(typeof rootReducer).toBe('function');
  });

  // Test 31: All slice reducers are properly combined
  it('should properly combine all slice reducers', () => {
    const state = store.getState();
    
    // Verify that each slice has its own independent state
    Object.keys(state).forEach(key => {
      expect(state[key]).toBeDefined();
    });
  });

  // Test 32: Root reducer maintains state consistency
  it('should maintain state consistency across actions', () => {
    const state1 = store.getState();
    
    store.dispatch({ type: 'ACTION_1' });
    const state2 = store.getState();
    
    store.dispatch({ type: 'ACTION_2' });
    const state3 = store.getState();
    
    // All states should have the same structure
    expect(Object.keys(state1).sort()).toEqual(Object.keys(state2).sort());
    expect(Object.keys(state2).sort()).toEqual(Object.keys(state3).sort());
  });

  // Test 33: Verify session slice structure
  it('should have correct session slice structure', () => {
    const state = store.getState();
    
    expect(state.session).toHaveProperty('sessionObj');
    expect(state.session.sessionObj).toHaveProperty('loading');
    expect(state.session.sessionObj).toHaveProperty('data');
    expect(state.session.sessionObj).toHaveProperty('error');
  });

  // Test 34: Verify enum slice structure
  it('should have correct enum slice structure', () => {
    const state = store.getState();
    
    expect(state.enum).toHaveProperty('enumObj');
    expect(state.enum.enumObj).toHaveProperty('loading');
    expect(state.enum.enumObj).toHaveProperty('data');
    expect(state.enum.enumObj).toHaveProperty('error');
  });

  // Test 35: Root reducer handles multiple sequential actions
  it('should handle multiple sequential actions correctly', () => {
    const actions = [
      { type: 'ACTION_1' },
      { type: 'ACTION_2' },
      { type: 'ACTION_3' }
    ];
    
    actions.forEach(action => {
      expect(() => store.dispatch(action)).not.toThrow();
    });
    
    const finalState = store.getState();
    expect(finalState).toBeDefined();
    expect(Object.keys(finalState)).toHaveLength(17);
  });

  // Test 36: State shape remains consistent
  it('should maintain consistent state shape', () => {
    const state = store.getState();
    const keys = Object.keys(state);
    
    // Dispatch some actions
    store.dispatch({ type: 'TEST_ACTION_1' });
    store.dispatch({ type: 'TEST_ACTION_2' });
    
    const newState = store.getState();
    const newKeys = Object.keys(newState);
    
    expect(keys).toEqual(newKeys);
  });

  // Test 37: Each slice maintains its own state
  it('should ensure each slice maintains its own state independently', () => {
    const state = store.getState();
    
    // Check that modifying one slice doesn't affect others
    const sessionState = state.session;
    const enumState = state.enum;
    
    expect(sessionState).not.toBe(enumState);
  });

  // Test 38: Root reducer type consistency
  it('should have consistent types across all slices', () => {
    const state = store.getState();
    
    // All slice states should be objects
    Object.values(state).forEach(sliceState => {
      expect(typeof sliceState).toBe('object');
      expect(sliceState).not.toBeNull();
    });
  });

  // Test 39: Verify all imports are used
  it('should use all imported slice reducers', () => {
    const state = store.getState();
    
    // Verify all slices from combineReducers are present
    const sliceCount = Object.keys(state).length;
    expect(sliceCount).toBe(17);
  });

  // Test 40: Root reducer serializable state
  it('should produce serializable state', () => {
    const state = store.getState();
    
    // State should be JSON serializable
    expect(() => JSON.stringify(state)).not.toThrow();
    expect(() => JSON.parse(JSON.stringify(state))).not.toThrow();
  });
});
