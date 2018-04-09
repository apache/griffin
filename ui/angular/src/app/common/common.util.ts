/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

export namespace CommonUtil {
    export interface DataAsset {
        Name: string;
        AssetType: string;
        Hdfs: HdfsType;
        Kafka: KafkaType;
    }
   
    interface TableType {
        tableName: string;
        Columns: Array<Column>;
    }

    interface Column {
        Name: string;
        Type: string;
        Id: number;
    }

    interface HdfsType {
        location: string;
        dbName: string;
        tableName: string;
    }

    interface KafkaType {
        BootstrapServers: string;
        GroupId: string;
        AutoOffsetReset: string;
        AutoCommitEnable: boolean;
        Topics: string;
        KeyType: string;
        ValueType: string;
        PreProcs: PreProc[];
        Updatable: boolean;
    }  

    interface PreProc {
        Id: number;
        DslType: string;
        Name: string;
        Rule: string;
        SparkSql: string;
        Details: PreProcDetail;
    }

    interface PreProcDetail {
        DfName: string;
        ColName: string;
    }      

    function createPreProc(Id?: number, DslType?: string, Name?:string, Rule?:string, SparkSql?:string, Details?:PreProcDetail): PreProc {
        return {
            Id,
            DslType,
            Name,
            Rule,
            SparkSql,
            Details
        }
    }

    export function addPreProc(Id: number) {
        return createPreProc(Id, '','','','', createPreProcDetail('',''));
    }    

    function createPreProcDetail(DfName?: string, ColName?: string): PreProcDetail {
        return {
            DfName,
            ColName    
        }
    }

    function createKafkaType(BootstrapServers?: string, GroupId?: string,
        AutoOffsetReset?:string, AutoCommitEnable?:boolean, Topics?:string, KeyType?:string,
        ValueType?:string, PreProcs?:PreProc[],Updatable?: boolean): KafkaType {
        return {
            BootstrapServers,
            GroupId,
            AutoOffsetReset,
            AutoCommitEnable,
            Topics,
            KeyType,
            ValueType,
            PreProcs,
            Updatable
        }
    }

    function createDataAsset(Name?: string, AssetType?: string, Hdfs?: HdfsType, Kafka?: KafkaType): DataAsset {        
        return {
            Name,
            AssetType,
            Hdfs,
            Kafka            
        }
    }

    export function createHdfsType(location?: string, dbName?: string, tableName?: string): HdfsType {
        return {
            location,
            dbName,
            tableName
        }
    }  
    
    export function initDataAsset() {   
        var hdfs = createHdfsType('','','')     
        var kafka = createKafkaType('','','',null, '','','',[createPreProc(0,'','','','',createPreProcDetail('',''))], null)
        return createDataAsset('', "kafka", hdfs, kafka);
    }

}  
