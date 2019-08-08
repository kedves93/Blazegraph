import { RawImage } from './../models/rawimage';
import { Injectable, Inject } from '@angular/core';
import { HttpHeaders, HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs/internal/Observable';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class UploadService {

  constructor(private http: HttpClient) {
  }

  uploadImage(rawImage: RawImage): Observable<any> {
    const reqHeaders = new HttpHeaders({
      'Content-Type': 'application/json'
    });
    const body = JSON.stringify(rawImage);
    return this.http.post<any>(environment.apiGateway + 'uploadimage', body, { headers: reqHeaders });
  }

  uploadText(text: string): Observable<any> {
    const reqHeaders = new HttpHeaders({
      'Content-Type': 'text/plain'
    });
    const body = text;
    return this.http.post<any>(environment.apiGateway + 'uploadtext', body, { headers: reqHeaders });
  }

}
