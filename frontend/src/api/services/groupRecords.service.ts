import {
    GroupRecordsApi,
    GroupRecordsListRequest,
    UpdateGroupRecordRequest,
} from '@/api';
import { config } from '@/api/services/config';

const api = new GroupRecordsApi(config);

export const groupRecordsService = {
    getList: async(organizationId: number, projectId: number, groupRecordsListRequest: GroupRecordsListRequest) => await api.groupRecordsList(organizationId, projectId, groupRecordsListRequest),
    get: async(organizationId: number, projectId: number, id: number) => await api.getGroupRecord(organizationId, projectId, id),
    updated: async(organizationId: number, projectId: number, id: number, updateGroupRecordRequest: UpdateGroupRecordRequest) => await api.updateGroupRecord(organizationId, projectId, id, updateGroupRecordRequest),
};