import { lineageApiUrl, relationsApiUrl } from "@api/apiUrlLinks/lineageApiUrl";
import { _get } from "./apiMethod";

const getLineageData = (guid: string, params: any) => {
  const config = {
    method: "GET",
    params: params,
  };
  return _get(lineageApiUrl(guid), config);
};

const addLineageData = (guid: string, data: any) => {
  const config = {
    method: "POST",
    params: {},
    data: data,
  };
  return _get(lineageApiUrl(guid), config);
};

const getRelationshipData = (options: any, params: any) => {
  const config = {
    method: "GET",
    params: params,
  };
  return _get(relationsApiUrl(options), config);
};

const saveRelationShip = (data: any) => {
  const config = {
    method: "PUT",
    params: {},
    data: data,
  };
  return _get(relationsApiUrl({}), config);
};

export {
  getLineageData,
  getRelationshipData,
  saveRelationShip,
  addLineageData,
};
