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

import { EntityStatus, isEntityModificationAllowed } from '../EntityStatus';

describe('isEntityModificationAllowed', () => {
    it('should return true for ACTIVE status', () => {
        expect(isEntityModificationAllowed(EntityStatus.ACTIVE)).toBe(true);
        expect(isEntityModificationAllowed('ACTIVE')).toBe(true);
    });

    it('should return false for DELETED status', () => {
        expect(isEntityModificationAllowed(EntityStatus.DELETED)).toBe(false);
        expect(isEntityModificationAllowed('DELETED')).toBe(false);
    });

    it('should return false for PURGED status', () => {
        expect(isEntityModificationAllowed(EntityStatus.PURGED)).toBe(false);
        expect(isEntityModificationAllowed('PURGED')).toBe(false);
    });

    it('should return true for undefined status', () => {
        expect(isEntityModificationAllowed(undefined)).toBe(true);
    });

    it('should return true for unknown status', () => {
        expect(isEntityModificationAllowed('UNKNOWN_STATUS')).toBe(true);
    });
});
