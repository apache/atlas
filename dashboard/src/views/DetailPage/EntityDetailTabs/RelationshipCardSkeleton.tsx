/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

function RelationshipCardSkeleton() {
	return (
		<div className='relationship-card-skeleton' aria-hidden='true'>
			<div className='relationship-card-skeleton__header'>
				<div className='relationship-card-skeleton__title-wrap'>
					<span className='relationship-card-skeleton__line relationship-card-skeleton__line--title' />
					<span className='relationship-card-skeleton__line relationship-card-skeleton__line--count' />
				</div>
				<div className='relationship-card-skeleton__actions'>
					<span className='relationship-card-skeleton__box relationship-card-skeleton__box--button' />
					<span className='relationship-card-skeleton__box relationship-card-skeleton__box--checkbox' />
				</div>
			</div>
			<div className='relationship-card-skeleton__content'>
				<div className='relationship-card-skeleton__body'>
					<span className='relationship-card-skeleton__line' />
					<span className='relationship-card-skeleton__line' />
					<span className='relationship-card-skeleton__line' />
					<span className='relationship-card-skeleton__line relationship-card-skeleton__line--short' />
				</div>
				<div className='relationship-card-skeleton__footer'>
					<span className='relationship-card-skeleton__line relationship-card-skeleton__line--footer' />
					<span className='relationship-card-skeleton__box relationship-card-skeleton__box--input' />
				</div>
			</div>
		</div>
	)
}

export default RelationshipCardSkeleton
