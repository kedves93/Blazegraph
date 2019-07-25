import { RawImage } from './../models/rawimage';
import { Injectable, Inject } from '@angular/core';
import { HttpHeaders, HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs/internal/Observable';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class UploadService {

  private headers: HttpHeaders;

  constructor(private http: HttpClient) {
    this.headers = new HttpHeaders({
      'Content-Type': 'application/json'
    });
  }

  uploadImage(rawImage: RawImage): Observable<any> {
    const body = JSON.stringify(rawImage);
    return this.http.post<any>(environment.apiGateway + 'uploadimage', body, { headers: this.headers });
  }

}
