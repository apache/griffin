import { TestBed, inject } from '@angular/core/testing';

import { GetMetricService } from './get-metric.service';

describe('GetMetricService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [GetMetricService]
    });
  });

  it('should be created', inject([GetMetricService], (service: GetMetricService) => {
    expect(service).toBeTruthy();
  }));
});
