# highchart-ui

```
    lein new figwheel highchart-ui -- --om
```

project.clj

```
  :cljsbuild {:builds
              [{:id "dev"
                :figwheel {
                           :websocket-host "192.168.1.2"
                           ;; :open-urls ["http://localhost:3449/index.html"]}
             }]}
```

resources/public/index.html
```
    <script src="http://code.highcharts.com/highcharts.js" type="text/javascript"></script>
```

http://192.168.1.2:3449
