import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DataassetComponent } from './dataasset.component';

describe('DataassetComponent', () => {
  let component: DataassetComponent;
  let fixture: ComponentFixture<DataassetComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DataassetComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DataassetComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
