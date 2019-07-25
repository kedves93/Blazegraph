import { Observable } from 'rxjs/internal/Observable';
import { HttpHeaders, HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class ExportService {

  constructor(private http: HttpClient) {
  }

  export(): Observable<any> {
    const params = new HttpParams().set('exportType', 'rdf');
    return this.http.get<any>(environment.apiGateway + 'export', { params });
  }
}
