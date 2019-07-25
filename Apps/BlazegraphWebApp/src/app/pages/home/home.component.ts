import { UploadService } from './../../services/upload.service';
import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { RawImage } from 'src/app/models/rawimage';
import { ExportService } from 'src/app/services/export.service';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.css'],
  encapsulation: ViewEncapsulation.None
})
export class HomeComponent implements OnInit {

  constructor(private uploadService: UploadService, private exportService: ExportService) { }

  ngOnInit() {
  }

  onUploadFiles(event, form) {
    const reader = new FileReader();
    reader.readAsDataURL(event.files[0]);
    reader.onload = () => {
      const rawImage = new RawImage();
      rawImage.name = event.files[0].name;
      rawImage.base64Content = reader.result.toString().split(',')[1];
      this.uploadService.uploadImage(rawImage).subscribe(
        x => form.clear(),
        error => console.log(error)
      );
    };
    reader.onerror = error => console.log(error);
  }

  onExportClick() {
    this.exportService.export().subscribe(
      x => {},
      error => console.log(error)
    );
  }

}
