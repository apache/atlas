// @ts-nocheck

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

import { isEmpty, isObject } from "./Utils";

const isString = (str: string) => {
  if (str != null && typeof str.valueOf() === "string") {
    return true;
  }
  return false;
};

const startsWith = (str: string, matchStr: string) => {
  if (str && matchStr && isString(str) && isString(matchStr)) {
    return str.lastIndexOf(matchStr, 0) === 0;
  } else {
    return;
  }
};

type Iteratee<T> = (value: T) => any;

function uniq<T>(array: T[], isSorted: boolean, iteratee?: Iteratee<T>): T[] {
  if (isSorted) {
    if (iteratee) {
      const seen = new Set<any>();
      return array.filter((item) => {
        const key = iteratee(item);
        if (seen.has(key)) {
          return false;
        } else {
          seen.add(key);
          return true;
        }
      });
    } else {
      return array.filter(
        (item, index, self) => index === self.findIndex((t) => t === item)
      );
    }
  } else {
    if (iteratee) {
      const seen = new Set<any>();
      return array.filter((item) => {
        const key = iteratee(item);
        if (seen.has(key)) {
          return false;
        } else {
          seen.add(key);
          return true;
        }
      });
    } else {
      return Array.from(new Set(array));
    }
  }
}

const toArrayifObject = (val: any) => {
  return isObject(val) ? [val] : val;
};

const cloneDeep = (param: any): any => {
  if (param === null) {
    return null;
  }

  if (
    typeof param === "string" ||
    typeof param === "number" ||
    typeof param === "boolean"
  ) {
    return param;
  }

  if (param instanceof Date) {
    return new Date(param);
  }

  if (Array.isArray(param)) {
    return param.map((item) => cloneDeep(item));
  }

  if (typeof param === "object") {
    const newObj = Object.create(Object.getPrototypeOf(param));
    Object.keys(param).forEach((key) => {
      newObj[key] = cloneDeep(param[key]);
    });
    return newObj;
  }

  return param;
};

const isEmptyObject = (obj: { [x: string]: string }) => {
  return (
    Object.keys(obj).length === 0 ||
    (Object.keys(obj).length === 1 && obj[""] === "")
  );
};

const extend = function () {
  let extended: any = {};
  let deep = false;
  let i = 0;
  let length = arguments.length;

  if (Object.prototype.toString.call(arguments[0]) === "[object Boolean]") {
    deep = arguments[0];
    i++;
  }

  const merge = function (obj: { [x: string]: any }) {
    for (let prop in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, prop)) {
        if (
          deep &&
          Object.prototype.toString.call(obj[prop]) === "[object Object]"
        ) {
          extended[prop] = extend(true, extended[prop], obj[prop]);
        } else {
          extended[prop] = obj[prop];
        }
      }
    }
  };

  for (; i < length; i++) {
    let obj = arguments[i];
    merge(obj);
  }

  return extended;
};

const sortByKeyWithUnderscoreFirst = (arr: any[], key: string) => {
  return arr.sort((a: { [x: string]: any }, b: { [x: string]: any }) => {
    const valueA = a[key];
    const valueB = b[key];

    const startsWithUnderscoreA = valueA.startsWith("_");
    const startsWithUnderscoreB = valueB.startsWith("_");

    return startsWithUnderscoreA === startsWithUnderscoreB
      ? valueA.localeCompare(valueB)
      : startsWithUnderscoreA
      ? -1
      : 1;
  });
};

const omit = (obj: any, props: any[]) => {
  const result = { ...obj };
  props.forEach(function (prop: string | number) {
    delete result[prop];
  });
  return result;
};

const invert = (myMap: { [s: string]: unknown } | ArrayLike<unknown>) => {
  let inverse = new Map();

  for (let [key, value] of Object.entries(myMap)) {
    inverse.set(value, key);
  }
  return inverse;
};

const numberFormatWithComma = (number: number) => {
  return new Intl.NumberFormat().format(number);
};

const numberFormatWithBytes = (number: number) => {
  if (number > -1) {
    if (number === 0) {
      return "0 Bytes";
    }
    let i = number == 0 ? 0 : Math.floor(Math.log(number) / Math.log(1024));
    if (i > 8) {
      return numberFormatWithComma(number);
    }
    return (
      Number((number / Math.pow(1024, i)).toFixed(2)) +
      " " +
      ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"][i]
    );
  } else {
    return number;
  }
};

const without = (array: any[], ...values: any[]) => {
  return array.filter((value) => !values.includes(value));
};

const union = (arr: string | any[], ...args: any[]) => [
  ...new Set(arr.concat(...args))
];

const customSortObj = (objData: { [x: string]: any }) => {
  return !isEmpty(objData)
    ? Object.keys(objData)
        .sort()
        .reduce((obj, key) => {
          obj[key] = objData[key];
          return obj;
        }, {})
    : {};
};

const isEmptyValueCheck = (value) => {
  return (
    value == null || // null or undefined
    (typeof value === "string" && value.trim() === "") ||
    (Array.isArray(value) && value.length === 0) ||
    (typeof value === "object" && Object.keys(value).length === 0)
  );
};

export {
  startsWith,
  uniq,
  toArrayifObject,
  cloneDeep,
  isEmptyObject,
  extend,
  sortByKeyWithUnderscoreFirst,
  omit,
  invert,
  numberFormatWithComma,
  numberFormatWithBytes,
  without,
  union,
  customSortObj,
  isEmptyValueCheck
};
