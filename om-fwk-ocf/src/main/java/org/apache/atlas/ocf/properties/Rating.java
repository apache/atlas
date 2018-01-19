/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.ocf.properties;


/**
 * Stores information about a rating connected to an asset.  Ratings provide informal feedback on the quality of assets
 * and can be added at any time.
 *
 * Ratings have the userId of the person who added it, a star rating and an optional review comment.
 *
 * The content of the rating is a personal judgement (which is why the user's id is in the object)
 * and there is no formal review of the ratings.  However, they can be used as a basis for crowd-sourcing
 * feedback to asset owners.
 */
public class Rating extends ElementHeader
{
    /*
     * Attributes of a Rating
     */
    private StarRating starRating = null;
    private String     review = null;
    private String     user = null;


    /**
     * Typical Constructor
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param starRating - StarRating enum - the star value for the rating.
     * @param review - String - review comments
     * @param user - String - user id of person providing the rating
     */
    public Rating(AssetDescriptor parentAsset,
                  ElementType     type,
                  String          guid,
                  String          url,
                  Classifications classifications,
                  StarRating      starRating,
                  String          review,
                  String          user)
    {
        super(parentAsset, type, guid, url, classifications);

        this.starRating = starRating;
        this.review = review;
        this.user = user;
    }

    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateRating - element to copy
     */
    public Rating(AssetDescriptor parentAsset, Rating templateRating)
    {
        /*
         * Save the parent asset description.
         */
        super(parentAsset, templateRating);

        if (templateRating != null)
        {
            /*
             * Copy the values from the supplied tag.
             */
            user = templateRating.getUser();
            starRating = templateRating.getStarRating();
            review = templateRating.getReview();
        }
    }


    /**
     * Return the user id of the person who created the rating.  Null means the user id is not known.
     *
     * @return String - user
     */
    public String getUser() {
        return user;
    }


    /**
     * Return the stars for the rating.
     *
     * @return StarRating - starRating
     */
    public StarRating getStarRating() {
        return starRating;
    }


    /**
     * Return the review comments - null means no review is available.
     *
     * @return String - review comments
     */
    public String getReview()
    {
        return review;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "Rating{" +
                "starRating=" + starRating +
                ", review='" + review + '\'' +
                ", user='" + user + '\'' +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}