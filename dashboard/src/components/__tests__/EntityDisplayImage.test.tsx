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

import React from 'react'
import { render, fireEvent } from '@testing-library/react'
import DisplayImage from '../EntityDisplayImage'
import * as Utils from '../../utils/Utils'

const mockGetEntityIconPath = jest.fn()

jest.mock('../../utils/Utils', () => ({
  getEntityIconPath: jest.fn()
}))

describe('EntityDisplayImage', () => {
  const entity = { guid: 'entity-1' }

  beforeEach(() => {
    jest.clearAllMocks()
    
    ;(Utils.getEntityIconPath as jest.Mock).mockImplementation(({ entityData, errorUrl }: { entityData: any, errorUrl?: string }) => {
      const result = errorUrl ? `${errorUrl}-fallback` : `/icons/${entityData.guid}.png`
      mockGetEntityIconPath({ entityData, errorUrl })
      return result
    })
  })

  it('renders primary image instantly', () => {
    const { container } = render(
      <DisplayImage entity={entity} width={20} height={20} />
    )

    const img = container.querySelector('img')
    expect(img).toBeInTheDocument()
    expect(img?.getAttribute('src')).toBe('/icons/entity-1.png')
    expect(img?.getAttribute('alt')).toBe('Entity Icon')
    expect(img?.getAttribute('id')).toBe('entity-1')
    expect(img?.getAttribute('data-cy')).toBe('entity-1')
  })

  it('switches to fallback image when native onError is triggered', () => {
    const { container } = render(
      <DisplayImage entity={entity} width={20} height={20} />
    )

    const img = container.querySelector('img')
    expect(img).toBeInTheDocument()
    expect(img?.getAttribute('src')).toBe('/icons/entity-1.png')

    // Trigger error natively
    fireEvent.error(img!)

    expect(img?.getAttribute('src')).toBe('/icons/entity-1.png-fallback')
  })

  it('renders Avatar when avatarDisplay is provided and handles fallback', () => {
    const { container } = render(
      <DisplayImage
        entity={entity}
        width={30}
        height={30}
        avatarDisplay={true}
      />
    )

    const avatar = container.querySelector('img[alt="entityImg"]')
    expect(avatar).toBeTruthy()
    expect(avatar?.getAttribute('src')).toBe('/icons/entity-1.png')

    // Trigger error natively
    fireEvent.error(avatar!)

    expect(avatar?.getAttribute('src')).toBe('/icons/entity-1.png-fallback')
  })

  it('handles isProcess prop', () => {
    const entityWithProcess = { guid: 'entity-2', isProcess: true }
    render(
      <DisplayImage entity={entityWithProcess} width={20} height={20} isProcess={true} />
    )

    expect(mockGetEntityIconPath).toHaveBeenCalledWith(
      expect.objectContaining({
        entityData: expect.objectContaining({ isProcess: true })
      })
    )
  })

  it('handles entity without isProcess prop but with isProcess passed', () => {
    render(
      <DisplayImage entity={entity} width={20} height={20} isProcess={false} />
    )

    expect(mockGetEntityIconPath).toHaveBeenCalledWith(
      expect.objectContaining({
        entityData: expect.objectContaining({ isProcess: false })
      })
    )
  })
})
