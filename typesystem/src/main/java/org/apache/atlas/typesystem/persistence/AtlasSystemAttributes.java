/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.typesystem.persistence;

import org.apache.atlas.typesystem.types.TypeSystem;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AtlasSystemAttributes {
    public  String createdBy;
    public  String modifiedBy;
    public Date createdTime;
    public Date modifiedTime;
    public SimpleDateFormat simpleDateFormat = TypeSystem.getInstance().getDateFormat();


    public AtlasSystemAttributes(String createdBy, String modifiedBy, Date createdTime, Date modifiedTime){
        this.createdBy = createdBy;
        this.modifiedBy = modifiedBy;
        this.createdTime = createdTime;
        this.modifiedTime = modifiedTime;
    }

    public AtlasSystemAttributes(){
        super();
    }

    public AtlasSystemAttributes(String createdBy, String modifiedBy, String createdTime, String modifiedTime){
        this.createdBy  = createdBy;
        this.modifiedBy = modifiedBy;

        try{
            this.createdTime = simpleDateFormat.parse(createdTime);
        }catch (ParseException e){
            //this.createdTime = new Date(0);
        }

        try{
            this.modifiedTime = simpleDateFormat.parse(modifiedTime);
        }catch (ParseException e){
            //this.modifiedTime = new Date(0);
        }
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AtlasSystemAttributes sys_attr = (AtlasSystemAttributes) o;

        if (!createdBy.equals(sys_attr.createdBy)) {
            return false;
        }
        if (!modifiedBy.equals(sys_attr.modifiedBy)) {
            return false;
        }
        if (!createdTime.equals(sys_attr.createdTime)) {
            return false;
        }

        if(!modifiedTime.equals(sys_attr.modifiedTime)){
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = createdBy.hashCode();
        result = 31 * result + modifiedBy.hashCode();
        result = 31 * result + createdTime.hashCode();
        result = 31 * result + modifiedTime.hashCode();
        return result;
    }

    public String getCreatedBy(){
        return createdBy;
    }

    public String getModifiedBy(){
        return modifiedBy;
    }

    public Date getCreatedTime(){
        return createdTime;
    }

    public Date getModifiedTime(){
        return modifiedTime;
    }
}
