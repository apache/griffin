/*
	Copyright (c) 2016 eBay Software Foundation.
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	    http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
 */
package com.ebay.oss.griffin.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import com.ebay.oss.griffin.vo.NotificationRecord;

@Service
public class NotificationServiceImpl implements NotificationService {
	private static List<NotificationRecord> records = new ArrayList<NotificationRecord>();
	private static int count = 1;

	@Override
	public void insert(NotificationRecord record) {
		record.setId(count++);
		records.add(0, record);

	}

	@Override
	public List<NotificationRecord> getAll() {
		return records;
	}

	@Override
	public void delete(NotificationRecord record) {
		int index = records.indexOf(record);
		if(index > -1){
			records.remove(index);
		}else if(record.getId()>0){
			delete(record.getId());
		}

	}

	@Override
	public void delete(int id) {
		int length = records.size();
		for(int i = 0; i < length; i ++){
			if(records.get(i).getId() == id){
				records.remove(i);
				break;
			}
		}

	}

	@Override
	public NotificationRecord get(int id) {
		int length = records.size();
		for(int i = 0; i < length; i ++){
			if(records.get(i).getId() == id){
				return records.get(i);
			}
		}
		return null;
	}

	@Override
	public List<NotificationRecord> getTop(int limit) {
		// TODO Auto-generated method stub
		return records.subList(0, limit);

	}

}
