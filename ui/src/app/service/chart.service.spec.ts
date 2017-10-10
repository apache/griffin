import { TestBed, inject } from '@angular/core/testing';

import { ChartService } from './chart.service';

describe('ChartService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [ChartService]
    });
  });

  it('should be created', inject([ChartService], (service: ChartService) => {
    expect(service).toBeTruthy();
  }));
});
