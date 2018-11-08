







<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
  <link rel="dns-prefetch" href="https://assets-cdn.github.com">
  <link rel="dns-prefetch" href="https://avatars0.githubusercontent.com">
  <link rel="dns-prefetch" href="https://avatars1.githubusercontent.com">
  <link rel="dns-prefetch" href="https://avatars2.githubusercontent.com">
  <link rel="dns-prefetch" href="https://avatars3.githubusercontent.com">
  <link rel="dns-prefetch" href="https://github-cloud.s3.amazonaws.com">
  <link rel="dns-prefetch" href="https://user-images.githubusercontent.com/">



  <link crossorigin="anonymous" media="all" integrity="sha512-mjQPRAh2Y9A0sPdZzipNfPO7PT4g06mk0uZs15DbL/vsNCRGx1uRzWVzls9MJCoy2yRNjaMmEVFKJDpCui00mA==" rel="stylesheet" href="https://assets-cdn.github.com/assets/frameworks-df973073d880f28fbbae0263fb1ef62b.css" />
  <link crossorigin="anonymous" media="all" integrity="sha512-k4rXi2xAgpvXlB7r/tZ1ski3o3AWXfn7Z6hx6C/g9CcFeM5miuGB8zJFRgQW5wDKRaNQfv42R9F707X/2WqAQg==" rel="stylesheet" href="https://assets-cdn.github.com/assets/github-2b520d809bcf76c745c815d9523f0a00.css" />
  
  
  <link crossorigin="anonymous" media="all" integrity="sha512-oq9Re9Urx17JAhj6uI9tKyz1nDNmkHrfNbiaVwDNPT5gi4+7A+z+/t/VLNWg7KBXCSRi4yZiQM6Rpu4rp1PwhQ==" rel="stylesheet" href="https://assets-cdn.github.com/assets/site-0be82e42e6ce84ef34fecbf8469a45aa.css" />
  

  <meta name="viewport" content="width=device-width">
  
  <title>ihme-modeling/dismod_ode.md at master · ihmeuw/ihme-modeling · GitHub</title>
    <meta name="description" content="Contribute to ihmeuw/ihme-modeling development by creating an account on GitHub.">
    <link rel="search" type="application/opensearchdescription+xml" href="/opensearch.xml" title="GitHub">
  <link rel="fluid-icon" href="https://github.com/fluidicon.png" title="GitHub">
  <meta property="fb:app_id" content="1401488693436528">

    
    <meta property="og:image" content="https://avatars1.githubusercontent.com/u/4683173?s=400&amp;v=4" /><meta property="og:site_name" content="GitHub" /><meta property="og:type" content="object" /><meta property="og:title" content="ihmeuw/ihme-modeling" /><meta property="og:url" content="https://github.com/ihmeuw/ihme-modeling" /><meta property="og:description" content="Contribute to ihmeuw/ihme-modeling development by creating an account on GitHub." />

  <link rel="assets" href="https://assets-cdn.github.com/">
  
  <meta name="pjax-timeout" content="1000">
  
  <meta name="request-id" content="FF34:1BB6:B94AA30:10547FA2:5BB25947" data-pjax-transient>


  

  <meta name="selected-link" value="repo_source" data-pjax-transient>

      <meta name="google-site-verification" content="KT5gs8h0wvaagLKAVWq8bbeNwnZZK1r1XQysX3xurLU">
    <meta name="google-site-verification" content="ZzhVyEFwb7w3e0-uOTltm8Jsck2F5StVihD0exw2fsA">
    <meta name="google-site-verification" content="GXs5KoUUkNCoaAZn7wPN-t01Pywp9M3sEjnt_3_ZWPc">

  <meta name="octolytics-host" content="collector.githubapp.com" /><meta name="octolytics-app-id" content="github" /><meta name="octolytics-event-url" content="https://collector.githubapp.com/github-external/browser_event" /><meta name="octolytics-dimension-request_id" content="FF34:1BB6:B94AA30:10547FA2:5BB25947" /><meta name="octolytics-dimension-region_edge" content="sea" /><meta name="octolytics-dimension-region_render" content="iad" />
<meta name="analytics-location" content="/&lt;user-name&gt;/&lt;repo-name&gt;/blob/show" data-pjax-transient="true" />



    <meta name="google-analytics" content="UA-3769691-2">


<meta class="js-ga-set" name="dimension1" content="Logged Out">



  

      <meta name="hostname" content="github.com">
    <meta name="user-login" content="">

      <meta name="expected-hostname" content="github.com">
    <meta name="js-proxy-site-detection-payload" content="M2FlNWYyMzZjNzVlNjYxM2NiZjJkMWM0NzM5YzJiYWI4ZjBkYmJkMmFmNDY1MTFmZWRhNWE0YTI4OWZlODBiYXx7InJlbW90ZV9hZGRyZXNzIjoiMjA1LjE3NS4xMTkuMTAwIiwicmVxdWVzdF9pZCI6IkZGMzQ6MUJCNjpCOTRBQTMwOjEwNTQ3RkEyOjVCQjI1OTQ3IiwidGltZXN0YW1wIjoxNTM4NDE0OTIwLCJob3N0IjoiZ2l0aHViLmNvbSJ9">

    <meta name="enabled-features" content="DASHBOARD_V2_LAYOUT_OPT_IN,EXPLORE_DISCOVER_REPOSITORIES,UNIVERSE_BANNER,MARKETPLACE_PLAN_RESTRICTION_EDITOR">

  <meta name="html-safe-nonce" content="f415017863ffdf7d26614e3367c68b36438c27cc">

  <meta http-equiv="x-pjax-version" content="50ecc2345c28dd181aeaee310d904a60">
  

      <link href="https://github.com/ihmeuw/ihme-modeling/commits/master.atom" rel="alternate" title="Recent Commits to ihme-modeling:master" type="application/atom+xml">

  <meta name="go-import" content="github.com/ihmeuw/ihme-modeling git https://github.com/ihmeuw/ihme-modeling.git">

  <meta name="octolytics-dimension-user_id" content="4683173" /><meta name="octolytics-dimension-user_login" content="ihmeuw" /><meta name="octolytics-dimension-repository_id" content="70285236" /><meta name="octolytics-dimension-repository_nwo" content="ihmeuw/ihme-modeling" /><meta name="octolytics-dimension-repository_public" content="true" /><meta name="octolytics-dimension-repository_is_fork" content="false" /><meta name="octolytics-dimension-repository_network_root_id" content="70285236" /><meta name="octolytics-dimension-repository_network_root_nwo" content="ihmeuw/ihme-modeling" /><meta name="octolytics-dimension-repository_explore_github_marketplace_ci_cta_shown" content="false" />


    <link rel="canonical" href="https://github.com/ihmeuw/ihme-modeling/blob/master/risk_factors_code/drugs_alcohol/alcohol_code/relative%20risk/dismod_ode.md" data-pjax-transient>


  <meta name="browser-stats-url" content="https://api.github.com/_private/browser/stats">

  <meta name="browser-errors-url" content="https://api.github.com/_private/browser/errors">

  <link rel="mask-icon" href="https://assets-cdn.github.com/pinned-octocat.svg" color="#000000">
  <link rel="icon" type="image/x-icon" class="js-site-favicon" href="https://assets-cdn.github.com/favicon.ico">

<meta name="theme-color" content="#1e2327">


  <meta name="u2f-support" content="true">

  <link rel="manifest" href="/manifest.json" crossOrigin="use-credentials">

  </head>

  <body class="logged-out env-production emoji-size-boost page-blob">
    

  <div class="position-relative js-header-wrapper ">
    <a href="#start-of-content" tabindex="1" class="px-2 py-4 bg-blue text-white show-on-focus js-skip-to-content">Skip to content</a>
    <div id="js-pjax-loader-bar" class="pjax-loader-bar"><div class="progress"></div></div>

    
    
    



        
<header class="Header header-logged-out  position-relative f4 py-3" role="banner">
  <div class="container-lg d-flex px-3">
    <div class="d-flex flex-justify-between flex-items-center">
      <a class="header-logo-invertocat my-0" href="https://github.com/" aria-label="Homepage" data-ga-click="(Logged out) Header, go to homepage, icon:logo-wordmark">
        <svg height="32" class="octicon octicon-mark-github" viewBox="0 0 16 16" version="1.1" width="32" aria-hidden="true"><path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"/></svg>
      </a>

    </div>

    <div class="HeaderMenu d-flex flex-justify-between flex-auto">
        <nav class="mt-0">
          <ul class="d-flex list-style-none">
              <li class="ml-2">
                <a class="js-selected-navigation-item HeaderNavlink px-0 py-2 m-0" data-ga-click="Header, click, Nav menu - item:features" data-selected-links="/features /features/project-management /features/code-review /features/project-management /features/integrations /features" href="/features">
                  Features
</a>              </li>
              <li class="ml-4">
                <a class="js-selected-navigation-item HeaderNavlink px-0 py-2 m-0" data-ga-click="Header, click, Nav menu - item:business" data-selected-links="/business /business/security /business/customers /business" href="/business">
                  Business
</a>              </li>

              <li class="ml-4">
                <a class="js-selected-navigation-item HeaderNavlink px-0 py-2 m-0" data-ga-click="Header, click, Nav menu - item:explore" data-selected-links="/explore /trending /trending/developers /integrations /integrations/feature/code /integrations/feature/collaborate /integrations/feature/ship showcases showcases_search showcases_landing /explore" href="/explore">
                  Explore
</a>              </li>

              <li class="ml-4">
                    <a class="js-selected-navigation-item HeaderNavlink px-0 py-2 m-0" data-ga-click="Header, click, Nav menu - item:marketplace" data-selected-links=" /marketplace" href="/marketplace">
                      Marketplace
</a>              </li>
              <li class="ml-4">
                <a class="js-selected-navigation-item HeaderNavlink px-0 py-2 m-0" data-ga-click="Header, click, Nav menu - item:pricing" data-selected-links="/pricing /pricing/developer /pricing/team /pricing/business-hosted /pricing/business-enterprise /pricing" href="/pricing">
                  Pricing
</a>              </li>
          </ul>
        </nav>

      <div class="d-flex">
          <div class="d-lg-flex flex-items-center mr-3">
            <div class="header-search scoped-search site-scoped-search js-site-search position-relative js-jump-to"
  role="combobox"
  aria-owns="jump-to-results"
  aria-label="Search or jump to"
  aria-haspopup="listbox"
  aria-expanded="false"
>
  <div class="position-relative">
    <!-- '"` --><!-- </textarea></xmp> --></option></form><form class="js-site-search-form" data-scope-type="Repository" data-scope-id="70285236" data-scoped-search-url="/ihmeuw/ihme-modeling/search" data-unscoped-search-url="/search" action="/ihmeuw/ihme-modeling/search" accept-charset="UTF-8" method="get"><input name="utf8" type="hidden" value="&#x2713;" />
      <label class="form-control header-search-wrapper header-search-wrapper-jump-to position-relative d-flex flex-justify-between flex-items-center js-chromeless-input-container">
        <input type="text"
          class="form-control header-search-input jump-to-field js-jump-to-field js-site-search-focus js-site-search-field is-clearable"
          data-hotkey="s,/"
          name="q"
          value=""
          placeholder="Search"
          data-unscoped-placeholder="Search GitHub"
          data-scoped-placeholder="Search"
          autocapitalize="off"
          aria-autocomplete="list"
          aria-controls="jump-to-results"
          data-jump-to-suggestions-path="/_graphql/GetSuggestedNavigationDestinations#csrf-token=MxzdWgMzNVC8/QQxoO+88k8gk8JEckJY1PZyhwyzoWElFXEv+JEOCt+mz/Mj6zkytb3zTLrgfjOVBHrbJAlLMw=="
          spellcheck="false"
          autocomplete="off"
          >
          <input type="hidden" class="js-site-search-type-field" name="type" >
            <img src="https://assets-cdn.github.com/images/search-shortcut-hint.svg" alt="" class="mr-2 header-search-key-slash">

            <div class="Box position-absolute overflow-hidden d-none jump-to-suggestions js-jump-to-suggestions-container">
              <ul class="d-none js-jump-to-suggestions-template-container">
                <li class="d-flex flex-justify-start flex-items-center p-0 f5 navigation-item js-navigation-item" role="option">
                  <a tabindex="-1" class="no-underline d-flex flex-auto flex-items-center p-2 jump-to-suggestions-path js-jump-to-suggestion-path js-navigation-open" href="">
                    <div class="jump-to-octicon js-jump-to-octicon mr-2 text-center d-none">
                      <svg height="16" width="16" class="octicon octicon-repo flex-shrink-0 js-jump-to-octicon-repo d-none" title="Repository" aria-label="Repository" viewBox="0 0 12 16" version="1.1" role="img"><path fill-rule="evenodd" d="M4 9H3V8h1v1zm0-3H3v1h1V6zm0-2H3v1h1V4zm0-2H3v1h1V2zm8-1v12c0 .55-.45 1-1 1H6v2l-1.5-1.5L3 16v-2H1c-.55 0-1-.45-1-1V1c0-.55.45-1 1-1h10c.55 0 1 .45 1 1zm-1 10H1v2h2v-1h3v1h5v-2zm0-10H2v9h9V1z"/></svg>
                      <svg height="16" width="16" class="octicon octicon-project flex-shrink-0 js-jump-to-octicon-project d-none" title="Project" aria-label="Project" viewBox="0 0 15 16" version="1.1" role="img"><path fill-rule="evenodd" d="M10 12h3V2h-3v10zm-4-2h3V2H6v8zm-4 4h3V2H2v12zm-1 1h13V1H1v14zM14 0H1a1 1 0 0 0-1 1v14a1 1 0 0 0 1 1h13a1 1 0 0 0 1-1V1a1 1 0 0 0-1-1z"/></svg>
                      <svg height="16" width="16" class="octicon octicon-search flex-shrink-0 js-jump-to-octicon-search d-none" title="Search" aria-label="Search" viewBox="0 0 16 16" version="1.1" role="img"><path fill-rule="evenodd" d="M15.7 13.3l-3.81-3.83A5.93 5.93 0 0 0 13 6c0-3.31-2.69-6-6-6S1 2.69 1 6s2.69 6 6 6c1.3 0 2.48-.41 3.47-1.11l3.83 3.81c.19.2.45.3.7.3.25 0 .52-.09.7-.3a.996.996 0 0 0 0-1.41v.01zM7 10.7c-2.59 0-4.7-2.11-4.7-4.7 0-2.59 2.11-4.7 4.7-4.7 2.59 0 4.7 2.11 4.7 4.7 0 2.59-2.11 4.7-4.7 4.7z"/></svg>
                    </div>

                    <img class="avatar mr-2 flex-shrink-0 js-jump-to-suggestion-avatar d-none" alt="" aria-label="Team" src="" width="28" height="28">

                    <div class="jump-to-suggestion-name js-jump-to-suggestion-name flex-auto overflow-hidden text-left no-wrap css-truncate css-truncate-target">
                    </div>

                    <div class="border rounded-1 flex-shrink-0 bg-gray px-1 text-gray-light ml-1 f6 d-none js-jump-to-badge-search">
                      <span class="js-jump-to-badge-search-text-default d-none" aria-label="in this repository">
                        In this repository
                      </span>
                      <span class="js-jump-to-badge-search-text-global d-none" aria-label="in all of GitHub">
                        All GitHub
                      </span>
                      <span aria-hidden="true" class="d-inline-block ml-1 v-align-middle">↵</span>
                    </div>

                    <div aria-hidden="true" class="border rounded-1 flex-shrink-0 bg-gray px-1 text-gray-light ml-1 f6 d-none d-on-nav-focus js-jump-to-badge-jump">
                      Jump to
                      <span class="d-inline-block ml-1 v-align-middle">↵</span>
                    </div>
                  </a>
                </li>
              </ul>
              <ul class="d-none js-jump-to-no-results-template-container">
                <li class="d-flex flex-justify-center flex-items-center p-3 f5 d-none">
                  <span class="text-gray">No suggested jump to results</span>
                </li>
              </ul>

              <ul id="jump-to-results" role="listbox" class="js-navigation-container jump-to-suggestions-results-container js-jump-to-suggestions-results-container" >
                <li class="d-flex flex-justify-center flex-items-center p-0 f5">
                  <img src="https://assets-cdn.github.com/images/spinners/octocat-spinner-128.gif" alt="Octocat Spinner Icon" class="m-2" width="28">
                </li>
              </ul>
            </div>
      </label>
</form>  </div>
</div>

          </div>

        <span class="d-inline-block">
            <div class="HeaderNavlink px-0 py-2 m-0">
              <a class="text-bold text-white no-underline" href="/login?return_to=%2Fihmeuw%2Fihme-modeling%2Fblob%2Fmaster%2Frisk_factors_code%2Fdrugs_alcohol%2Falcohol_code%2Frelative%2520risk%2Fdismod_ode.md" data-ga-click="(Logged out) Header, clicked Sign in, text:sign-in">Sign in</a>
                <span class="text-gray">or</span>
                <a class="text-bold text-white no-underline" href="/join?source=header-repo" data-ga-click="(Logged out) Header, clicked Sign up, text:sign-up">Sign up</a>
            </div>
        </span>
      </div>
    </div>
  </div>
</header>

  </div>

  <div id="start-of-content" class="show-on-focus"></div>

    <div id="js-flash-container">


</div>



  <div role="main" class="application-main ">
        <div itemscope itemtype="http://schema.org/SoftwareSourceCode" class="">
    <div id="js-repo-pjax-container" data-pjax-container >
      






  



  <div class="pagehead repohead instapaper_ignore readability-menu experiment-repo-nav  ">
    <div class="repohead-details-container clearfix container">

      <ul class="pagehead-actions">
  <li>
      <a href="/login?return_to=%2Fihmeuw%2Fihme-modeling"
    class="btn btn-sm btn-with-count tooltipped tooltipped-s"
    aria-label="You must be signed in to watch a repository" rel="nofollow">
    <svg class="octicon octicon-eye v-align-text-bottom" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M8.06 2C3 2 0 8 0 8s3 6 8.06 6C13 14 16 8 16 8s-3-6-7.94-6zM8 12c-2.2 0-4-1.78-4-4 0-2.2 1.8-4 4-4 2.22 0 4 1.8 4 4 0 2.22-1.78 4-4 4zm2-4c0 1.11-.89 2-2 2-1.11 0-2-.89-2-2 0-1.11.89-2 2-2 1.11 0 2 .89 2 2z"/></svg>
    Watch
  </a>
  <a class="social-count" href="/ihmeuw/ihme-modeling/watchers"
     aria-label="10 users are watching this repository">
    10
  </a>

  </li>

  <li>
      <a href="/login?return_to=%2Fihmeuw%2Fihme-modeling"
    class="btn btn-sm btn-with-count tooltipped tooltipped-s"
    aria-label="You must be signed in to star a repository" rel="nofollow">
    <svg class="octicon octicon-star v-align-text-bottom" viewBox="0 0 14 16" version="1.1" width="14" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M14 6l-4.9-.64L7 1 4.9 5.36 0 6l3.6 3.26L2.67 14 7 11.67 11.33 14l-.93-4.74L14 6z"/></svg>
    Star
  </a>

    <a class="social-count js-social-count" href="/ihmeuw/ihme-modeling/stargazers"
      aria-label="12 users starred this repository">
      12
    </a>

  </li>

  <li>
      <a href="/login?return_to=%2Fihmeuw%2Fihme-modeling"
        class="btn btn-sm btn-with-count tooltipped tooltipped-s"
        aria-label="You must be signed in to fork a repository" rel="nofollow">
        <svg class="octicon octicon-repo-forked v-align-text-bottom" viewBox="0 0 10 16" version="1.1" width="10" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M8 1a1.993 1.993 0 0 0-1 3.72V6L5 8 3 6V4.72A1.993 1.993 0 0 0 2 1a1.993 1.993 0 0 0-1 3.72V6.5l3 3v1.78A1.993 1.993 0 0 0 5 15a1.993 1.993 0 0 0 1-3.72V9.5l3-3V4.72A1.993 1.993 0 0 0 8 1zM2 4.2C1.34 4.2.8 3.65.8 3c0-.65.55-1.2 1.2-1.2.65 0 1.2.55 1.2 1.2 0 .65-.55 1.2-1.2 1.2zm3 10c-.66 0-1.2-.55-1.2-1.2 0-.65.55-1.2 1.2-1.2.65 0 1.2.55 1.2 1.2 0 .65-.55 1.2-1.2 1.2zm3-10c-.66 0-1.2-.55-1.2-1.2 0-.65.55-1.2 1.2-1.2.65 0 1.2.55 1.2 1.2 0 .65-.55 1.2-1.2 1.2z"/></svg>
        Fork
      </a>

    <a href="/ihmeuw/ihme-modeling/network/members" class="social-count"
       aria-label="13 users forked this repository">
      13
    </a>
  </li>
</ul>

      <h1 class="public ">
  <svg class="octicon octicon-repo" viewBox="0 0 12 16" version="1.1" width="12" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M4 9H3V8h1v1zm0-3H3v1h1V6zm0-2H3v1h1V4zm0-2H3v1h1V2zm8-1v12c0 .55-.45 1-1 1H6v2l-1.5-1.5L3 16v-2H1c-.55 0-1-.45-1-1V1c0-.55.45-1 1-1h10c.55 0 1 .45 1 1zm-1 10H1v2h2v-1h3v1h5v-2zm0-10H2v9h9V1z"/></svg>
  <span class="author" itemprop="author"><a class="url fn" rel="author" href="/ihmeuw">ihmeuw</a></span><!--
--><span class="path-divider">/</span><!--
--><strong itemprop="name"><a data-pjax="#js-repo-pjax-container" href="/ihmeuw/ihme-modeling">ihme-modeling</a></strong>

</h1>

    </div>
    
<nav class="reponav js-repo-nav js-sidenav-container-pjax container"
     itemscope
     itemtype="http://schema.org/BreadcrumbList"
     role="navigation"
     data-pjax="#js-repo-pjax-container">

  <span itemscope itemtype="http://schema.org/ListItem" itemprop="itemListElement">
    <a class="js-selected-navigation-item selected reponav-item" itemprop="url" data-hotkey="g c" data-selected-links="repo_source repo_downloads repo_commits repo_releases repo_tags repo_branches repo_packages /ihmeuw/ihme-modeling" href="/ihmeuw/ihme-modeling">
      <svg class="octicon octicon-code" viewBox="0 0 14 16" version="1.1" width="14" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M9.5 3L8 4.5 11.5 8 8 11.5 9.5 13 14 8 9.5 3zm-5 0L0 8l4.5 5L6 11.5 2.5 8 6 4.5 4.5 3z"/></svg>
      <span itemprop="name">Code</span>
      <meta itemprop="position" content="1">
</a>  </span>

    <span itemscope itemtype="http://schema.org/ListItem" itemprop="itemListElement">
      <a itemprop="url" data-hotkey="g i" class="js-selected-navigation-item reponav-item" data-selected-links="repo_issues repo_labels repo_milestones /ihmeuw/ihme-modeling/issues" href="/ihmeuw/ihme-modeling/issues">
        <svg class="octicon octicon-issue-opened" viewBox="0 0 14 16" version="1.1" width="14" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"/></svg>
        <span itemprop="name">Issues</span>
        <span class="Counter">0</span>
        <meta itemprop="position" content="2">
</a>    </span>

  <span itemscope itemtype="http://schema.org/ListItem" itemprop="itemListElement">
    <a data-hotkey="g p" itemprop="url" class="js-selected-navigation-item reponav-item" data-selected-links="repo_pulls checks /ihmeuw/ihme-modeling/pulls" href="/ihmeuw/ihme-modeling/pulls">
      <svg class="octicon octicon-git-pull-request" viewBox="0 0 12 16" version="1.1" width="12" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M11 11.28V5c-.03-.78-.34-1.47-.94-2.06C9.46 2.35 8.78 2.03 8 2H7V0L4 3l3 3V4h1c.27.02.48.11.69.31.21.2.3.42.31.69v6.28A1.993 1.993 0 0 0 10 15a1.993 1.993 0 0 0 1-3.72zm-1 2.92c-.66 0-1.2-.55-1.2-1.2 0-.65.55-1.2 1.2-1.2.65 0 1.2.55 1.2 1.2 0 .65-.55 1.2-1.2 1.2zM4 3c0-1.11-.89-2-2-2a1.993 1.993 0 0 0-1 3.72v6.56A1.993 1.993 0 0 0 2 15a1.993 1.993 0 0 0 1-3.72V4.72c.59-.34 1-.98 1-1.72zm-.8 10c0 .66-.55 1.2-1.2 1.2-.65 0-1.2-.55-1.2-1.2 0-.65.55-1.2 1.2-1.2.65 0 1.2.55 1.2 1.2zM2 4.2C1.34 4.2.8 3.65.8 3c0-.65.55-1.2 1.2-1.2.65 0 1.2.55 1.2 1.2 0 .65-.55 1.2-1.2 1.2z"/></svg>
      <span itemprop="name">Pull requests</span>
      <span class="Counter">0</span>
      <meta itemprop="position" content="3">
</a>  </span>


    <a data-hotkey="g b" class="js-selected-navigation-item reponav-item" data-selected-links="repo_projects new_repo_project repo_project /ihmeuw/ihme-modeling/projects" href="/ihmeuw/ihme-modeling/projects">
      <svg class="octicon octicon-project" viewBox="0 0 15 16" version="1.1" width="15" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M10 12h3V2h-3v10zm-4-2h3V2H6v8zm-4 4h3V2H2v12zm-1 1h13V1H1v14zM14 0H1a1 1 0 0 0-1 1v14a1 1 0 0 0 1 1h13a1 1 0 0 0 1-1V1a1 1 0 0 0-1-1z"/></svg>
      Projects
      <span class="Counter" >0</span>
</a>


  <a class="js-selected-navigation-item reponav-item" data-selected-links="repo_graphs repo_contributors dependency_graph pulse alerts /ihmeuw/ihme-modeling/pulse" href="/ihmeuw/ihme-modeling/pulse">
    <svg class="octicon octicon-graph" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M16 14v1H0V0h1v14h15zM5 13H3V8h2v5zm4 0H7V3h2v10zm4 0h-2V6h2v7z"/></svg>
    Insights
</a>

</nav>


  </div>

<div class="container new-discussion-timeline experiment-repo-nav  ">
  <div class="repository-content ">

    
  <a class="d-none js-permalink-shortcut" data-hotkey="y" href="/ihmeuw/ihme-modeling/blob/636e4f3971a23230dfe01f72df4270545628fa1b/risk_factors_code/drugs_alcohol/alcohol_code/relative%20risk/dismod_ode.md">Permalink</a>

  <!-- blob contrib key: blob_contributors:v21:94794817ced283b02627bf0cae80df1f -->

      <div class="signup-prompt-bg rounded-1">
      <div class="signup-prompt p-4 text-center mb-4 rounded-1">
        <div class="position-relative">
          <!-- '"` --><!-- </textarea></xmp> --></option></form><form action="/site/dismiss_signup_prompt" accept-charset="UTF-8" method="post"><input name="utf8" type="hidden" value="&#x2713;" /><input type="hidden" name="authenticity_token" value="8HQpmZXPM+OhocV+me8ul2YXGbR2vA6gjlVHZ/yagwjUSUzlysKJoIy4wT0DR9x/V48Pe/jUEW1kC2jiURtwHQ==" />
            <button type="submit" class="position-absolute top-0 right-0 btn-link link-gray" data-ga-click="(Logged out) Sign up prompt, clicked Dismiss, text:dismiss">
              Dismiss
            </button>
</form>          <h3 class="pt-2">Join GitHub today</h3>
          <p class="col-6 mx-auto">GitHub is home to over 28 million developers working together to host and review code, manage projects, and build software together.</p>
          <a class="btn btn-primary" href="/join?source=prompt-blob-show" data-ga-click="(Logged out) Sign up prompt, clicked Sign up, text:sign-up">Sign up</a>
        </div>
      </div>
    </div>


  <div class="file-navigation">
    
<div class="select-menu branch-select-menu js-menu-container js-select-menu float-left">
  <button class=" btn btn-sm select-menu-button js-menu-target css-truncate" data-hotkey="w"
    
    type="button" aria-label="Switch branches or tags" aria-expanded="false" aria-haspopup="true">
      <i>Branch:</i>
      <span class="js-select-button css-truncate-target">master</span>
  </button>

  <div class="select-menu-modal-holder js-menu-content js-navigation-container" data-pjax>

    <div class="select-menu-modal">
      <div class="select-menu-header">
        <svg class="octicon octicon-x js-menu-close" role="img" aria-label="Close" viewBox="0 0 12 16" version="1.1" width="12" height="16"><path fill-rule="evenodd" d="M7.48 8l3.75 3.75-1.48 1.48L6 9.48l-3.75 3.75-1.48-1.48L4.52 8 .77 4.25l1.48-1.48L6 6.52l3.75-3.75 1.48 1.48L7.48 8z"/></svg>
        <span class="select-menu-title">Switch branches/tags</span>
      </div>

      <div class="select-menu-filters">
        <div class="select-menu-text-filter">
          <input type="text" aria-label="Filter branches/tags" id="context-commitish-filter-field" class="form-control js-filterable-field js-navigation-enable" placeholder="Filter branches/tags">
        </div>
        <div class="select-menu-tabs">
          <ul>
            <li class="select-menu-tab">
              <a href="#" data-tab-filter="branches" data-filter-placeholder="Filter branches/tags" class="js-select-menu-tab" role="tab">Branches</a>
            </li>
            <li class="select-menu-tab">
              <a href="#" data-tab-filter="tags" data-filter-placeholder="Find a tag…" class="js-select-menu-tab" role="tab">Tags</a>
            </li>
          </ul>
        </div>
      </div>

      <div class="select-menu-list select-menu-tab-bucket js-select-menu-tab-bucket" data-tab-filter="branches" role="menu">

        <div data-filterable-for="context-commitish-filter-field" data-filterable-type="substring">


            <a class="select-menu-item js-navigation-item js-navigation-open selected"
               href="/ihmeuw/ihme-modeling/blob/master/risk_factors_code/drugs_alcohol/alcohol_code/relative%20risk/dismod_ode.md"
               data-name="master"
               data-skip-pjax="true"
               rel="nofollow">
              <svg class="octicon octicon-check select-menu-item-icon" viewBox="0 0 12 16" version="1.1" width="12" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M12 5l-8 8-4-4 1.5-1.5L4 10l6.5-6.5L12 5z"/></svg>
              <span class="select-menu-item-text css-truncate-target js-select-menu-filter-text">
                master
              </span>
            </a>
        </div>

          <div class="select-menu-no-results">Nothing to show</div>
      </div>

      <div class="select-menu-list select-menu-tab-bucket js-select-menu-tab-bucket" data-tab-filter="tags">
        <div data-filterable-for="context-commitish-filter-field" data-filterable-type="substring">


        </div>

        <div class="select-menu-no-results">Nothing to show</div>
      </div>

    </div>
  </div>
</div>

    <div class="BtnGroup float-right">
      <a href="/ihmeuw/ihme-modeling/find/master"
            class="js-pjax-capture-input btn btn-sm BtnGroup-item"
            data-pjax
            data-hotkey="t">
        Find file
      </a>
      <clipboard-copy for="blob-path" class="btn btn-sm BtnGroup-item">
        Copy path
      </clipboard-copy>
    </div>
    <div id="blob-path" class="breadcrumb">
      <span class="repo-root js-repo-root"><span class="js-path-segment"><a data-pjax="true" href="/ihmeuw/ihme-modeling"><span>ihme-modeling</span></a></span></span><span class="separator">/</span><span class="js-path-segment"><a data-pjax="true" href="/ihmeuw/ihme-modeling/tree/master/risk_factors_code"><span>risk_factors_code</span></a></span><span class="separator">/</span><span class="js-path-segment"><a data-pjax="true" href="/ihmeuw/ihme-modeling/tree/master/risk_factors_code/drugs_alcohol"><span>drugs_alcohol</span></a></span><span class="separator">/</span><span class="js-path-segment"><a data-pjax="true" href="/ihmeuw/ihme-modeling/tree/master/risk_factors_code/drugs_alcohol/alcohol_code"><span>alcohol_code</span></a></span><span class="separator">/</span><span class="js-path-segment"><a data-pjax="true" href="/ihmeuw/ihme-modeling/tree/master/risk_factors_code/drugs_alcohol/alcohol_code/relative%20risk"><span>relative risk</span></a></span><span class="separator">/</span><strong class="final-path">dismod_ode.md</strong>
    </div>
  </div>


  
  <div class="commit-tease">
      <span class="float-right">
        <a class="commit-tease-sha" href="/ihmeuw/ihme-modeling/commit/636e4f3971a23230dfe01f72df4270545628fa1b" data-pjax>
          636e4f3
        </a>
        <relative-time datetime="2018-09-21T23:18:48Z">Sep 21, 2018</relative-time>
      </span>
      <div>
        <a rel="contributor" data-skip-pjax="true" data-hovercard-type="user" data-hovercard-url="/hovercards?user_id=22654099" data-octo-click="hovercard-link-click" data-octo-dimensions="link_type:self" href="/healthdata"><img class="avatar" src="https://avatars3.githubusercontent.com/u/22654099?s=40&amp;v=4" width="20" height="20" alt="@healthdata" /></a>
        <a class="user-mention" rel="contributor" data-hovercard-type="user" data-hovercard-url="/hovercards?user_id=22654099" data-octo-click="hovercard-link-click" data-octo-dimensions="link_type:self" href="/healthdata">healthdata</a>
          <a data-pjax="true" title="refreshed code" class="message" href="/ihmeuw/ihme-modeling/commit/636e4f3971a23230dfe01f72df4270545628fa1b">refreshed code</a>
      </div>

    <div class="commit-tease-contributors">
      
<details class="details-reset details-overlay details-overlay-dark lh-default text-gray-dark float-left mr-2" id="blob_contributors_box">
  <summary class="btn-link" aria-haspopup="dialog"  >
    
    <span><strong>1</strong> contributor</span>
  </summary>
  <details-dialog class="Box Box--overlay d-flex flex-column anim-fade-in fast " aria-label="Users who have contributed to this file">
    <div class="Box-header">
      <button class="Box-btn-octicon btn-octicon float-right" type="button" aria-label="Close dialog" data-close-dialog>
        <svg class="octicon octicon-x" viewBox="0 0 12 16" version="1.1" width="12" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M7.48 8l3.75 3.75-1.48 1.48L6 9.48l-3.75 3.75-1.48-1.48L4.52 8 .77 4.25l1.48-1.48L6 6.52l3.75-3.75 1.48 1.48L7.48 8z"/></svg>
      </button>
      <h3 class="Box-title">Users who have contributed to this file</h3>
    </div>
    
        <ul class="list-style-none overflow-auto">
            <li class="Box-row">
              <a class="link-gray-dark no-underline" href="/healthdata">
                <img class="avatar mr-2" alt="" src="https://avatars3.githubusercontent.com/u/22654099?s=40&amp;v=4" width="20" height="20" />
                healthdata
</a>            </li>
        </ul>

  </details-dialog>
</details>
      
    </div>
  </div>



  <div class="file">
    <div class="file-header">
  <div class="file-actions">

    <div class="BtnGroup">
      <a id="raw-url" class="btn btn-sm BtnGroup-item" href="/ihmeuw/ihme-modeling/raw/master/risk_factors_code/drugs_alcohol/alcohol_code/relative%20risk/dismod_ode.md">Raw</a>
        <a class="btn btn-sm js-update-url-with-hash BtnGroup-item" data-hotkey="b" href="/ihmeuw/ihme-modeling/blame/master/risk_factors_code/drugs_alcohol/alcohol_code/relative%20risk/dismod_ode.md">Blame</a>
      <a rel="nofollow" class="btn btn-sm BtnGroup-item" href="/ihmeuw/ihme-modeling/commits/master/risk_factors_code/drugs_alcohol/alcohol_code/relative%20risk/dismod_ode.md">History</a>
    </div>

        <a class="btn-octicon tooltipped tooltipped-nw"
           href="https://desktop.github.com"
           aria-label="Open this file in GitHub Desktop"
           data-ga-click="Repository, open with desktop, type:mac">
            <svg class="octicon octicon-device-desktop" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M15 2H1c-.55 0-1 .45-1 1v9c0 .55.45 1 1 1h5.34c-.25.61-.86 1.39-2.34 2h8c-1.48-.61-2.09-1.39-2.34-2H15c.55 0 1-.45 1-1V3c0-.55-.45-1-1-1zm0 9H1V3h14v8z"/></svg>
        </a>

        <button type="button" class="btn-octicon disabled tooltipped tooltipped-nw"
          aria-label="You must be signed in to make or propose changes">
          <svg class="octicon octicon-pencil" viewBox="0 0 14 16" version="1.1" width="14" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M0 12v3h3l8-8-3-3-8 8zm3 2H1v-2h1v1h1v1zm10.3-9.3L12 6 9 3l1.3-1.3a.996.996 0 0 1 1.41 0l1.59 1.59c.39.39.39 1.02 0 1.41z"/></svg>
        </button>
        <button type="button" class="btn-octicon btn-octicon-danger disabled tooltipped tooltipped-nw"
          aria-label="You must be signed in to make or propose changes">
          <svg class="octicon octicon-trashcan" viewBox="0 0 12 16" version="1.1" width="12" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M11 2H9c0-.55-.45-1-1-1H5c-.55 0-1 .45-1 1H2c-.55 0-1 .45-1 1v1c0 .55.45 1 1 1v9c0 .55.45 1 1 1h7c.55 0 1-.45 1-1V5c.55 0 1-.45 1-1V3c0-.55-.45-1-1-1zm-1 12H3V5h1v8h1V5h1v8h1V5h1v8h1V5h1v9zm1-10H2V3h9v1z"/></svg>
        </button>
  </div>

  <div class="file-info">
      <span class="file-mode" title="File mode">executable file</span>
      <span class="file-info-divider"></span>
      322 lines (236 sloc)
      <span class="file-info-divider"></span>
    9.58 KB
  </div>
</div>

    
  <div id="readme" class="readme blob instapaper_body">
    <article class="markdown-body entry-content" itemprop="text"><h3><a id="user-content-run-dismod-ode-to-produce-relative-risks-for-alcohol" class="anchor" aria-hidden="true" href="#run-dismod-ode-to-produce-relative-risks-for-alcohol"><svg class="octicon octicon-link" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M4 9h1v1H4c-1.5 0-3-1.69-3-3.5S2.55 3 4 3h4c1.45 0 3 1.69 3 3.5 0 1.41-.91 2.72-2 3.25V8.59c.58-.45 1-1.27 1-2.09C10 5.22 8.98 4 8 4H4c-.98 0-2 1.22-2 2.5S3 9 4 9zm9-3h-1v1h1c1 0 2 1.22 2 2.5S13.98 12 13 12H9c-.98 0-2-1.22-2-2.5 0-.83.42-1.64 1-2.09V6.25c-1.09.53-2 1.84-2 3.25C6 11.31 7.55 13 9 13h4c1.45 0 3-1.69 3-3.5S14.5 6 13 6z"></path></svg></a>Run dismod ODE to produce relative risks for alcohol</h3>
<div class="highlight highlight-source-python"><pre><span class="pl-k">%</span>load_ext rpy2.ipython</pre></div>
<div class="highlight highlight-source-python"><pre><span class="pl-k">%%</span>R

<span class="pl-c"><span class="pl-c">#</span>Prepare necessary inputs for Dismod ODE</span>
<span class="pl-c"><span class="pl-c">#</span>Need:</span>
<span class="pl-c"><span class="pl-c">#</span> data_in, effect_in, plain_in, rate_in, value_in</span>

<span class="pl-c"><span class="pl-c">#</span>Load packages</span>
library(data.table)

<span class="pl-c"><span class="pl-c">#</span>Set up code options</span>
cause <span class="pl-k">&lt;</span><span class="pl-k">-</span> 
version   <span class="pl-k">&lt;</span><span class="pl-k">-</span> 

inputs    <span class="pl-k">&lt;</span><span class="pl-k">-</span> 
outputs   <span class="pl-k">&lt;</span><span class="pl-k">-</span> 
templates <span class="pl-k">&lt;</span><span class="pl-k">-</span> 

dose            <span class="pl-k">&lt;</span><span class="pl-k">-</span> c(<span class="pl-c1">0</span>, <span class="pl-c1">30</span>, <span class="pl-c1">50</span>, <span class="pl-c1">70</span>, <span class="pl-c1">100</span>, <span class="pl-c1">150</span>)   <span class="pl-c"><span class="pl-c">#</span>Points for desired output</span>
covariates      <span class="pl-k">&lt;</span><span class="pl-k">-</span> c(<span class="pl-s"><span class="pl-pds">"</span>ones<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>measure<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>former_drinkers<span class="pl-pds">"</span></span>)  <span class="pl-c"><span class="pl-c">#</span>Need at minimum sex and intercept</span>
midpoint        <span class="pl-k">&lt;</span><span class="pl-k">-</span> F <span class="pl-c"><span class="pl-c">#</span>Use midpoint for comparing</span>

smoothing <span class="pl-k">&lt;</span><span class="pl-k">-</span> c(<span class="pl-c1">.05</span>, <span class="pl-c1">.05</span>, <span class="pl-c1">.05</span>)

<span class="pl-c"><span class="pl-c">#</span>Theo recommended scaling values between 0 &amp; 100</span>
adjust <span class="pl-k">&lt;</span><span class="pl-k">-</span> <span class="pl-c1">TRUE</span>

<span class="pl-k">if</span> (adjust <span class="pl-k">==</span> <span class="pl-c1">TRUE</span>){
    adjustment_factor   <span class="pl-k">&lt;</span><span class="pl-k">-</span> <span class="pl-c1">100</span><span class="pl-k">/</span><span class="pl-c1">max</span>(dose)
    dose                <span class="pl-k">&lt;</span><span class="pl-k">-</span> dose<span class="pl-k">*</span>adjustment_factor
}

<span class="pl-c"><span class="pl-c">#</span>Set up Dismod ODE options</span>

sample          <span class="pl-k">&lt;</span><span class="pl-k">-</span> <span class="pl-c1">5000</span>
sample_interval <span class="pl-k">&lt;</span><span class="pl-k">-</span> <span class="pl-c1">20</span>

<span class="pl-c"><span class="pl-c">#</span>Make necessary dismod files</span>

<span class="pl-c"><span class="pl-c">#</span>Read raw data</span>
raw <span class="pl-k">&lt;</span><span class="pl-k">-</span> fread(paste0(inputs, <span class="pl-s"><span class="pl-pds">"</span>data_in_<span class="pl-pds">"</span></span>, cause, <span class="pl-s"><span class="pl-pds">"</span>.csv<span class="pl-pds">"</span></span>))

<span class="pl-c"><span class="pl-c">#</span>Format for dismod</span>
raw <span class="pl-k">&lt;</span><span class="pl-k">-</span> raw[gday_lower <span class="pl-k">!=</span> <span class="pl-s"><span class="pl-pds">"</span>EX<span class="pl-pds">"</span></span>,]
raw <span class="pl-k">&lt;</span><span class="pl-k">-</span> raw[is_outlier <span class="pl-k">!=</span> <span class="pl-c1">1</span>, ]
raw <span class="pl-k">&lt;</span><span class="pl-k">-</span> raw[source_type <span class="pl-k">!=</span><span class="pl-s"><span class="pl-pds">"</span>case-control<span class="pl-pds">"</span></span>,]

<span class="pl-c"><span class="pl-c">#</span>Pull out list of studies to produce random effects in rate_in</span>
studies <span class="pl-k">&lt;</span><span class="pl-k">-</span> unique(raw<span class="pl-ii">$</span>study)

raw[, gday_lower := <span class="pl-k">as</span>.numeric(gday_lower)]
raw[, gday_upper := <span class="pl-k">as</span>.numeric(gday_upper)]

<span class="pl-c"><span class="pl-c">#</span>Transform values to be between 0-100</span>
<span class="pl-k">if</span> (adjust<span class="pl-k">==</span><span class="pl-c1">TRUE</span>){
    raw[, gday_lower := gday_lower<span class="pl-k">*</span>adjustment_factor]
    raw[, gday_upper := gday_upper<span class="pl-k">*</span>adjustment_factor]
}

raw[<span class="pl-k">is</span>.na(gday_lower), gday_lower:=<span class="pl-c1">0</span>]
raw[<span class="pl-k">is</span>.na(gday_upper), gday_upper:=gday_lower<span class="pl-k">+</span><span class="pl-c1">30</span>]

raw[, ]

raw[, <span class="pl-c1">super</span>:=<span class="pl-s"><span class="pl-pds">"</span>none<span class="pl-pds">"</span></span>]
raw[, region:=<span class="pl-s"><span class="pl-pds">"</span>none<span class="pl-pds">"</span></span>]
raw[, subreg:=study]
raw[, age_lower:=gday_lower]
raw[, age_upper:=gday_upper]

<span class="pl-c"><span class="pl-c">#</span>Use midpoint instead of upper/lower if setting is on</span>
<span class="pl-k">if</span> (midpoint <span class="pl-k">==</span> T){
    raw[, hold := age_lower <span class="pl-k">+</span> (age_upper <span class="pl-k">-</span> age_lower)<span class="pl-k">/</span><span class="pl-c1">2</span>]
    raw[, <span class="pl-bu">`:=`</span>(age_lower = hold, age_upper = hold)]
}

<span class="pl-c"><span class="pl-c">#</span>Set covariates</span>
raw[, x_sex:=<span class="pl-c1">0</span>]
raw[, x_ones:=<span class="pl-c1">1</span>]
raw[, x_former_drinkers:=former_drinkers]

<span class="pl-c"><span class="pl-c">#</span>raw[, x_source_type:=0]</span>

raw[, x_measure:=<span class="pl-c1">0</span>]
raw[measure<span class="pl-k">==</span><span class="pl-s"><span class="pl-pds">"</span>OR<span class="pl-pds">"</span></span>, x_measure:=<span class="pl-c1">1</span>]
raw[measure<span class="pl-k">==</span><span class="pl-s"><span class="pl-pds">"</span>HR<span class="pl-pds">"</span></span>, x_measure:=<span class="pl-c1">2</span>]
raw[measure<span class="pl-k">==</span><span class="pl-s"><span class="pl-pds">"</span>SMR<span class="pl-pds">"</span></span>, x_measure:=<span class="pl-c1">3</span>]

<span class="pl-c"><span class="pl-c">#</span>Add necessary generic variables for dismod</span>
raw[sex<span class="pl-k">==</span><span class="pl-s"><span class="pl-pds">"</span>Male<span class="pl-pds">"</span></span>, x_sex:=<span class="pl-c1">0.5</span>]
raw[sex<span class="pl-k">==</span><span class="pl-s"><span class="pl-pds">"</span>Female<span class="pl-pds">"</span></span>, x_sex:=<span class="pl-k">-</span><span class="pl-c1">0.5</span>]
raw[, time_lower:=<span class="pl-c1">2000</span>]
raw[, time_upper:=<span class="pl-c1">2000</span>]
raw[, data_like:= <span class="pl-s"><span class="pl-pds">"</span>log_gaussian<span class="pl-pds">"</span></span>]
raw[, integrand:= <span class="pl-s"><span class="pl-pds">"</span>incidence<span class="pl-pds">"</span></span>]
raw[, meas_value:=value]
raw[, meas_stdev:= (log(upper) <span class="pl-k">-</span> log(meas_value))<span class="pl-k">/</span><span class="pl-c1">1.96</span>]

keep <span class="pl-k">&lt;</span><span class="pl-k">-</span> c(<span class="pl-s"><span class="pl-pds">"</span>age_lower<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>age_upper<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>time_lower<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>time_upper<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>super<span class="pl-pds">"</span></span>,<span class="pl-s"><span class="pl-pds">"</span>region<span class="pl-pds">"</span></span>,<span class="pl-s"><span class="pl-pds">"</span>subreg<span class="pl-pds">"</span></span>,<span class="pl-s"><span class="pl-pds">"</span>data_like<span class="pl-pds">"</span></span>,<span class="pl-s"><span class="pl-pds">"</span>integrand<span class="pl-pds">"</span></span>,<span class="pl-s"><span class="pl-pds">"</span>meas_value<span class="pl-pds">"</span></span>,
          <span class="pl-s"><span class="pl-pds">"</span>meas_stdev<span class="pl-pds">"</span></span>, names(raw)[names(raw) <span class="pl-k">%</span>like<span class="pl-k">%</span> <span class="pl-s"><span class="pl-pds">"</span>x_<span class="pl-pds">"</span></span>])

raw <span class="pl-k">&lt;</span><span class="pl-k">-</span> raw[, keep, <span class="pl-k">with</span>=<span class="pl-c1">FALSE</span>]

<span class="pl-c"><span class="pl-c">#</span>Make data_in</span>
data_in <span class="pl-k">&lt;</span><span class="pl-k">-</span> data.table(<span class="pl-v">age_lower</span> <span class="pl-k">=</span> dose[<span class="pl-c1">1</span>:length(dose)<span class="pl-k">-</span><span class="pl-c1">1</span>],
                      <span class="pl-v">age_upper</span> <span class="pl-k">=</span> dose[<span class="pl-c1">2</span>:length(dose)],
                      <span class="pl-v">time_lower</span> <span class="pl-k">=</span> <span class="pl-c1">2000</span>,
                      <span class="pl-v">time_upper</span> <span class="pl-k">=</span> <span class="pl-c1">2000</span>,
                      <span class="pl-v">super</span> <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">"</span>none<span class="pl-pds">"</span></span>,
                      <span class="pl-v">region</span> <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">"</span>none<span class="pl-pds">"</span></span>,
                      <span class="pl-v">subreg</span> <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">"</span>none<span class="pl-pds">"</span></span>,
                      <span class="pl-v">data_like</span> <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">"</span>log_gaussian<span class="pl-pds">"</span></span>,
                      <span class="pl-v">integrand</span> <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">"</span>mtall<span class="pl-pds">"</span></span>,
                      <span class="pl-v">meas_value</span> <span class="pl-k">=</span> <span class="pl-c1">0.01</span>,
                      <span class="pl-v">meas_stdev</span> <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">"</span>inf<span class="pl-pds">"</span></span>)
    
<span class="pl-k">for</span> (cov <span class="pl-k">in</span> covariates){
    data_in[, paste0(<span class="pl-s"><span class="pl-pds">"</span>x_<span class="pl-pds">"</span></span>, cov) := <span class="pl-c1">0</span>]
}

data_in <span class="pl-k">&lt;</span><span class="pl-k">-</span> rbind(raw, data_in, <span class="pl-v">fill</span><span class="pl-k">=</span><span class="pl-c1">TRUE</span>)
data_in[meas_stdev <span class="pl-k">&lt;=</span> <span class="pl-c1">0</span>, meas_stdev:=<span class="pl-c1">0.01</span>]

<span class="pl-c"><span class="pl-c">#</span>Make rate_in</span>

rate_vars <span class="pl-k">&lt;</span><span class="pl-k">-</span> c(<span class="pl-s"><span class="pl-pds">"</span>iota<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>chi<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>omega<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>rho<span class="pl-pds">"</span></span>)
deriv_vars <span class="pl-k">&lt;</span><span class="pl-k">-</span> c(<span class="pl-s"><span class="pl-pds">"</span>diota<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>dchi<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>domega<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>drho<span class="pl-pds">"</span></span>)

rate_in <span class="pl-k">&lt;</span><span class="pl-k">-</span> data.table(expand.grid(<span class="pl-v">type</span><span class="pl-k">=</span>rate_vars, <span class="pl-v">age</span><span class="pl-k">=</span>dose))
deriv_df <span class="pl-k">&lt;</span><span class="pl-k">-</span> data.table(expand.grid(<span class="pl-v">type</span><span class="pl-k">=</span>deriv_vars, <span class="pl-v">age</span><span class="pl-k">=</span>dose[<span class="pl-c1">1</span>:length(dose)<span class="pl-k">-</span><span class="pl-c1">1</span>]))

rate_in <span class="pl-k">&lt;</span><span class="pl-k">-</span> rbind(rate_in[order(<span class="pl-c1">type</span>)], deriv_df[order(<span class="pl-c1">type</span>)])

rate_in[, <span class="pl-bu">`:=`</span> (lower = <span class="pl-s"><span class="pl-pds">"</span>_inf<span class="pl-pds">"</span></span>,
               upper = <span class="pl-s"><span class="pl-pds">"</span>inf<span class="pl-pds">"</span></span>,
               mean = <span class="pl-c1">0</span>,
               std = <span class="pl-s"><span class="pl-pds">"</span>inf<span class="pl-pds">"</span></span>)]

    <span class="pl-c"><span class="pl-c">#</span>Change derivatives to be (-inf, inf)</span>
rate_in[<span class="pl-ii">!</span><span class="pl-c1">type</span> <span class="pl-k">%</span><span class="pl-k">in</span><span class="pl-k">%</span> c(<span class="pl-s"><span class="pl-pds">"</span>dchi<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>diota<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>domega<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>drho<span class="pl-pds">"</span></span>), <span class="pl-bu">`:=`</span>(lower = <span class="pl-c1">0</span>, upper = <span class="pl-c1">0</span>)]

    <span class="pl-c"><span class="pl-c">#</span>Make sure iota is bounded from (0, inf) and choose reasonable mean</span>
rate_in[<span class="pl-c1">type</span><span class="pl-k">==</span><span class="pl-s"><span class="pl-pds">"</span>iota<span class="pl-pds">"</span></span>, <span class="pl-bu">`:=`</span>(lower = <span class="pl-c1">0</span>,
                            upper = <span class="pl-s"><span class="pl-pds">"</span>inf<span class="pl-pds">"</span></span>,
                            mean = <span class="pl-c1">1</span>)]
    
    <span class="pl-c"><span class="pl-c">#</span>Bind iota at 0 to 1 (since this is a RR and 0 is the counterfactual)</span>
rate_in[(<span class="pl-c1">type</span><span class="pl-k">==</span><span class="pl-s"><span class="pl-pds">"</span>iota<span class="pl-pds">"</span></span>) <span class="pl-k">&amp;</span> (age<span class="pl-k">==</span><span class="pl-c1">0</span>), <span class="pl-bu">`:=`</span>(lower = <span class="pl-c1">1</span>,
                                        upper = <span class="pl-c1">1</span>,
                                        mean = <span class="pl-c1">1</span>)]

<span class="pl-c"><span class="pl-c">#</span>Make effect_in</span>
effect_in <span class="pl-k">&lt;</span><span class="pl-k">-</span> fread(paste0(templates, <span class="pl-s"><span class="pl-pds">"</span>effect_in.csv<span class="pl-pds">"</span></span>), <span class="pl-v">colClasses</span><span class="pl-k">=</span>c(<span class="pl-v">std</span><span class="pl-k">=</span><span class="pl-s"><span class="pl-pds">"</span>object<span class="pl-pds">"</span></span>))

    <span class="pl-c"><span class="pl-c">#</span>Add on random effects</span>
add_on_studies <span class="pl-k">&lt;</span><span class="pl-k">-</span> data.table(<span class="pl-v">integrand</span> <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">"</span>incidence<span class="pl-pds">"</span></span>,
                     <span class="pl-v">name</span> <span class="pl-k">=</span> studies,
                     <span class="pl-v">effect</span> <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">"</span>subreg<span class="pl-pds">"</span></span>,
                     <span class="pl-v">lower</span> <span class="pl-k">=</span> <span class="pl-k">-</span><span class="pl-c1">2</span>,
                     <span class="pl-v">upper</span> <span class="pl-k">=</span> <span class="pl-c1">2</span>,
                     <span class="pl-v">mean</span> <span class="pl-k">=</span> <span class="pl-c1">0</span>,
                     <span class="pl-v">std</span> <span class="pl-k">=</span> <span class="pl-c1">1</span>)

add_on_covariates <span class="pl-k">&lt;</span><span class="pl-k">-</span> data.table(<span class="pl-v">integrand</span> <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">"</span>incidence<span class="pl-pds">"</span></span>,
                               <span class="pl-v">effect</span> <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">"</span>xcov<span class="pl-pds">"</span></span>,
                               <span class="pl-v">name</span> <span class="pl-k">=</span> paste0(<span class="pl-s"><span class="pl-pds">"</span>x_<span class="pl-pds">"</span></span>, covariates[<span class="pl-ii">!</span>covariates <span class="pl-k">%</span><span class="pl-k">in</span><span class="pl-k">%</span> c(<span class="pl-s"><span class="pl-pds">"</span>sex<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>ones<span class="pl-pds">"</span></span>)]),
                               <span class="pl-v">lower</span> <span class="pl-k">=</span> <span class="pl-k">-</span><span class="pl-c1">2</span>,
                               <span class="pl-v">upper</span> <span class="pl-k">=</span> <span class="pl-c1">2</span>,
                               <span class="pl-v">mean</span> <span class="pl-k">=</span> <span class="pl-c1">0</span>,
                               <span class="pl-v">std</span> <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">"</span>inf<span class="pl-pds">"</span></span>)

<span class="pl-k">if</span> (add_on_covariates[, name] <span class="pl-k">==</span> <span class="pl-s"><span class="pl-pds">"</span>x_<span class="pl-pds">"</span></span>){
    add_on_covariates <span class="pl-k">&lt;</span><span class="pl-k">-</span> data.table()
}

effect_in <span class="pl-k">&lt;</span><span class="pl-k">-</span> rbind(effect_in, add_on_studies)
effect_in <span class="pl-k">&lt;</span><span class="pl-k">-</span> rbind(effect_in, add_on_covariates)

<span class="pl-c"><span class="pl-c">#</span>Make plain_in</span>
plain_in <span class="pl-k">&lt;</span><span class="pl-k">-</span> data.table(<span class="pl-v">name</span> <span class="pl-k">=</span> c(<span class="pl-s"><span class="pl-pds">"</span>p_zero<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>xi_omega<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>xi_chi<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>xi_iota<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>xi_rho<span class="pl-pds">"</span></span>), <span class="pl-v">lower</span> <span class="pl-k">=</span> c(<span class="pl-c1">0</span>, <span class="pl-c1">0.1</span>, <span class="pl-c1">0.1</span>, smoothing[<span class="pl-c1">1</span>], <span class="pl-c1">0.1</span>),
                       <span class="pl-v">upper</span> <span class="pl-k">=</span> c(<span class="pl-c1">1</span>, <span class="pl-c1">0.1</span>, <span class="pl-c1">0.1</span>, smoothing[<span class="pl-c1">3</span>], <span class="pl-c1">0.1</span>), <span class="pl-v">mean</span> <span class="pl-k">=</span> c(<span class="pl-c1">0.1</span>, <span class="pl-c1">0.1</span>, <span class="pl-c1">0.1</span>, smoothing[<span class="pl-c1">2</span>], <span class="pl-c1">0.1</span>), <span class="pl-v">std</span> <span class="pl-k">=</span> c(<span class="pl-s"><span class="pl-pds">"</span>inf<span class="pl-pds">"</span></span>,<span class="pl-s"><span class="pl-pds">"</span>inf<span class="pl-pds">"</span></span>,<span class="pl-s"><span class="pl-pds">"</span>inf<span class="pl-pds">"</span></span>,<span class="pl-s"><span class="pl-pds">"</span>inf<span class="pl-pds">"</span></span>,<span class="pl-s"><span class="pl-pds">"</span>inf<span class="pl-pds">"</span></span>))

<span class="pl-c"><span class="pl-c">#</span>Make value_in</span>
value_in <span class="pl-k">&lt;</span><span class="pl-k">-</span> fread(paste0(templates, <span class="pl-s"><span class="pl-pds">"</span>value_in.csv<span class="pl-pds">"</span></span>))
value_in[name <span class="pl-k">==</span> <span class="pl-s"><span class="pl-pds">"</span>num_sample<span class="pl-pds">"</span></span>,  value := sample]
value_in[name <span class="pl-k">==</span> <span class="pl-s"><span class="pl-pds">"</span>sample_interval<span class="pl-pds">"</span></span>, value := sample_interval]

<span class="pl-c"><span class="pl-c">#</span>Ship off final datasets</span>
<span class="pl-c1">dir</span>.create(outputs, <span class="pl-v">showWarnings</span> <span class="pl-k">=</span> <span class="pl-c1">FALSE</span>, <span class="pl-v">recursive</span><span class="pl-k">=</span><span class="pl-c1">TRUE</span>)

write.csv(data_in, paste0(outputs, <span class="pl-s"><span class="pl-pds">"</span>/data_in.csv<span class="pl-pds">"</span></span>), row.names<span class="pl-k">=</span><span class="pl-c1">FALSE</span>)
write.csv(rate_in, paste0(outputs, <span class="pl-s"><span class="pl-pds">"</span>/rate_in.csv<span class="pl-pds">"</span></span>), row.names<span class="pl-k">=</span><span class="pl-c1">FALSE</span>)
write.csv(effect_in, paste0(outputs, <span class="pl-s"><span class="pl-pds">"</span>/effect_in.csv<span class="pl-pds">"</span></span>), row.names<span class="pl-k">=</span><span class="pl-c1">FALSE</span>)
write.csv(plain_in, paste0(outputs, <span class="pl-s"><span class="pl-pds">"</span>/plain_in.csv<span class="pl-pds">"</span></span>), row.names<span class="pl-k">=</span><span class="pl-c1">FALSE</span>)
write.csv(value_in, paste0(outputs, <span class="pl-s"><span class="pl-pds">"</span>/value_in.csv<span class="pl-pds">"</span></span>), row.names<span class="pl-k">=</span><span class="pl-c1">FALSE</span>)

<span class="pl-c1">print</span>(outputs)

<span class="pl-bu">```python</span>
<span class="pl-k">import</span> os
os.chdir(<span class="pl-s"><span class="pl-pds">"</span><span class="pl-pds">"</span></span>)

! 
!  scale_beta<span class="pl-k">=</span>false


<span class="pl-bu">```python</span>


<span class="pl-k">%%</span>R 

library(data.table)

df <span class="pl-k">&lt;</span><span class="pl-k">-</span> fread()

<span class="pl-c"><span class="pl-c">#</span>Only hold onto last 1000 draws and iota's</span>

df <span class="pl-k">&lt;</span><span class="pl-k">-</span> df[(dim(df)[<span class="pl-c1">1</span>] <span class="pl-k">-</span> <span class="pl-c1">999</span>):(dim(df)[<span class="pl-c1">1</span>])]
df <span class="pl-k">&lt;</span><span class="pl-k">-</span> df[, (colnames(df) <span class="pl-k">%</span>like<span class="pl-k">%</span> <span class="pl-s"><span class="pl-pds">"</span>iota<span class="pl-pds">"</span></span>) <span class="pl-k">&amp;</span> !(colnames(df) <span class="pl-k">%</span>like<span class="pl-k">%</span> <span class="pl-s"><span class="pl-pds">"</span>xi<span class="pl-pds">"</span></span>), <span class="pl-k">with</span>=<span class="pl-c1">FALSE</span> ]

<span class="pl-c"><span class="pl-c">#</span>Reshape long for easier manipulation, add on draw column, rename columns and remove strings from exposure column</span>
df <span class="pl-k">&lt;</span><span class="pl-k">-</span> melt(df)
names(df) <span class="pl-k">&lt;</span><span class="pl-k">-</span> c(<span class="pl-s"><span class="pl-pds">"</span>exposure<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>rr<span class="pl-pds">"</span></span>)

df[, exposure := gsub(<span class="pl-s"><span class="pl-pds">"</span>iota_<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span><span class="pl-pds">"</span></span>, df<span class="pl-ii">$</span>exposure)]
df[, exposure := <span class="pl-k">as</span>.numeric(exposure)]

<span class="pl-c"><span class="pl-c">#</span>Modify exposure to match adjustment factor</span>
<span class="pl-k">if</span> (adjust<span class="pl-k">==</span><span class="pl-c1">TRUE</span>){
    df[, exposure := exposure<span class="pl-k">/</span>adjustment_factor]
}

draws <span class="pl-k">&lt;</span><span class="pl-k">-</span> rep(seq(<span class="pl-c1">0</span>, <span class="pl-c1">999</span>), length(unique(df<span class="pl-ii">$</span>exposure)))
df[, draw :=draws]

<span class="pl-c"><span class="pl-c">#</span>Make supplemental dataset holding mean, upper, lower for graphing below.</span>

df[, <span class="pl-bu">`:=`</span>(mean  = mean(.<span class="pl-c1">SD</span><span class="pl-ii">$</span>rr),
          lower = quantile(.<span class="pl-c1">SD</span><span class="pl-ii">$</span>rr, <span class="pl-c1">0.025</span>),
          upper = quantile(.<span class="pl-c1">SD</span><span class="pl-ii">$</span>rr, <span class="pl-c1">0.975</span>)),
    by=c(<span class="pl-s"><span class="pl-pds">"</span>exposure<span class="pl-pds">"</span></span>)]

df_compressed <span class="pl-k">&lt;</span><span class="pl-k">-</span> unique(df[, .(exposure, mean, lower, upper)])

write.csv(df[, .(exposure, draw, rr)], paste0(outputs, <span class="pl-s"><span class="pl-pds">"</span>/rr_draws.csv<span class="pl-pds">"</span></span>), row.names<span class="pl-k">=</span><span class="pl-c1">FALSE</span>)
write.csv(df_compressed, paste0(outputs, <span class="pl-s"><span class="pl-pds">"</span>/rr_summary.csv<span class="pl-pds">"</span></span>), row.names<span class="pl-k">=</span><span class="pl-c1">FALSE</span>)</pre></div>
<div class="highlight highlight-source-python"><pre><span class="pl-k">%%</span>R

library(ggplot2)

df_compressed <span class="pl-k">&lt;</span><span class="pl-k">-</span> fread(paste0(outputs, <span class="pl-s"><span class="pl-pds">"</span>/rr_summary.csv<span class="pl-pds">"</span></span>))
df_data       <span class="pl-k">&lt;</span><span class="pl-k">-</span> fread(paste0(outputs, <span class="pl-s"><span class="pl-pds">"</span>/data_in.csv<span class="pl-pds">"</span></span>))

<span class="pl-c"><span class="pl-c">#</span>Format raw data</span>
df_data <span class="pl-k">&lt;</span><span class="pl-k">-</span> df_data[integrand <span class="pl-k">!=</span> <span class="pl-s"><span class="pl-pds">"</span>mtall<span class="pl-pds">"</span></span>,]
df_data[, meas_stdev:=<span class="pl-k">as</span>.numeric(meas_stdev)]

setnames(df_data, c(<span class="pl-s"><span class="pl-pds">"</span>age_lower<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>age_upper<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>meas_value<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>subreg<span class="pl-pds">"</span></span>), c(<span class="pl-s"><span class="pl-pds">"</span>exposure_lower<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>exposure_upper<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>mean<span class="pl-pds">"</span></span>, <span class="pl-s"><span class="pl-pds">"</span>study<span class="pl-pds">"</span></span>))

<span class="pl-k">if</span> (adjust<span class="pl-k">==</span><span class="pl-c1">TRUE</span>){
    df_data[, exposure_lower := exposure_lower<span class="pl-k">/</span>adjustment_factor]
    df_data[, exposure_upper := exposure_upper<span class="pl-k">/</span>adjustment_factor]
}

df_data[, lower:= log(mean) <span class="pl-k">-</span> <span class="pl-c1">1.96</span><span class="pl-k">*</span>meas_stdev]
df_data[, upper:= log(mean) <span class="pl-k">+</span> <span class="pl-c1">1.96</span><span class="pl-k">*</span>meas_stdev]

df_data[, <span class="pl-bu">`:=`</span>(lower = exp(lower),
              upper = exp(upper))]

df_data <span class="pl-k">&lt;</span><span class="pl-k">-</span> df_data[, .(study, exposure_lower, exposure_upper, mean, lower, upper)]
df_data[, exposure := exposure_lower <span class="pl-k">+</span> (exposure_upper <span class="pl-k">-</span> exposure_lower)<span class="pl-k">/</span><span class="pl-c1">2</span>]

plot <span class="pl-k">&lt;</span><span class="pl-k">-</span> ggplot(df_compressed, aes(<span class="pl-v">x</span><span class="pl-k">=</span>exposure, <span class="pl-v">y</span><span class="pl-k">=</span>mean)) <span class="pl-k">+</span>
        geom_point() <span class="pl-k">+</span> 
        geom_line() <span class="pl-k">+</span> 
        geom_point(<span class="pl-v">data</span><span class="pl-k">=</span>df_data, aes(<span class="pl-v">color</span><span class="pl-k">=</span>study), <span class="pl-v">alpha</span><span class="pl-k">=</span><span class="pl-c1">0.5</span>, <span class="pl-v">size</span><span class="pl-k">=</span><span class="pl-c1">0.5</span>) <span class="pl-k">+</span>
        geom_errorbar(<span class="pl-v">data</span><span class="pl-k">=</span>df_data, aes(<span class="pl-v">ymin</span><span class="pl-k">=</span>lower, <span class="pl-v">ymax</span><span class="pl-k">=</span>upper, <span class="pl-v">color</span><span class="pl-k">=</span>study), <span class="pl-v">alpha</span><span class="pl-k">=</span><span class="pl-c1">0.3</span>) <span class="pl-k">+</span>
        geom_errorbarh(<span class="pl-v">data</span><span class="pl-k">=</span>df_data, aes(<span class="pl-v">xmin</span><span class="pl-k">=</span>exposure_lower, <span class="pl-v">xmax</span><span class="pl-k">=</span>exposure_upper, <span class="pl-v">color</span><span class="pl-k">=</span>study), <span class="pl-v">alpha</span><span class="pl-k">=</span><span class="pl-c1">0.3</span>) <span class="pl-k">+</span>
        geom_ribbon(<span class="pl-v">data</span><span class="pl-k">=</span>df_compressed, aes(<span class="pl-v">ymin</span><span class="pl-k">=</span>lower, <span class="pl-v">ymax</span><span class="pl-k">=</span>upper), <span class="pl-v">alpha</span><span class="pl-k">=</span><span class="pl-c1">0.2</span>, <span class="pl-v">fill</span><span class="pl-k">=</span><span class="pl-s"><span class="pl-pds">'</span>blue<span class="pl-pds">'</span></span>) <span class="pl-k">+</span>
        geom_line(<span class="pl-v">y</span><span class="pl-k">=</span><span class="pl-c1">1</span>, <span class="pl-v">color</span><span class="pl-k">=</span><span class="pl-s"><span class="pl-pds">'</span>red<span class="pl-pds">'</span></span>) <span class="pl-k">+</span>
        coord_cartesian(<span class="pl-v">xlim</span><span class="pl-k">=</span>c(<span class="pl-c1">0</span>, <span class="pl-c1">150</span>), <span class="pl-v">ylim</span><span class="pl-k">=</span>c(<span class="pl-c1">0.5</span>, <span class="pl-c1">4</span>)) <span class="pl-k">+</span>
        ylab(<span class="pl-s"><span class="pl-pds">"</span>RR<span class="pl-pds">"</span></span>) <span class="pl-k">+</span>
        ggtitle(paste(cause)) <span class="pl-k">+</span>
        guides(<span class="pl-v">color</span><span class="pl-k">=</span>F) <span class="pl-k">+</span>
        <span class="pl-c"><span class="pl-c">#</span>guides(col = guide_legend(nrow=50, byrow=TRUE)) +</span>
        theme(legend.direction<span class="pl-k">=</span><span class="pl-s"><span class="pl-pds">'</span>vertical<span class="pl-pds">'</span></span>, legend.key.size<span class="pl-k">=</span> unit(<span class="pl-c1">.2</span>, <span class="pl-s"><span class="pl-pds">'</span>cm<span class="pl-pds">'</span></span>), legend.text <span class="pl-k">=</span> element_text(<span class="pl-v">size</span><span class="pl-k">=</span><span class="pl-c1">10</span>), legend.justification <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">'</span>top<span class="pl-pds">'</span></span>)
        
<span class="pl-c1">print</span>(plot)

<span class="pl-c"><span class="pl-c">#</span>Calculate RMSE for values lower than 100</span>
pred <span class="pl-k">&lt;</span><span class="pl-k">-</span> approx(df_compressed<span class="pl-ii">$</span>exposure, df_compressed<span class="pl-ii">$</span>mean, df_data<span class="pl-ii">$</span>exposure)<span class="pl-ii">$</span>y
df_data[(exposure <span class="pl-k">&gt;</span> <span class="pl-c1">0</span>), pred:=pred]

rmse <span class="pl-k">&lt;</span><span class="pl-k">-</span> sqrt(mean((df_data<span class="pl-ii">$</span>mean <span class="pl-k">-</span> df_data<span class="pl-ii">$</span>pred)<span class="pl-k">^</span><span class="pl-c1">2</span>, na.rm<span class="pl-k">=</span><span class="pl-c1">TRUE</span>))
<span class="pl-c1">print</span>(paste0(<span class="pl-s"><span class="pl-pds">"</span>RMSE: <span class="pl-pds">"</span></span>, rmse))

<span class="pl-c"><span class="pl-c">#</span>Calculate in-sample coverage, for points within range</span>
pred_lower <span class="pl-k">&lt;</span><span class="pl-k">-</span> approx(df_compressed<span class="pl-ii">$</span>exposure, df_compressed<span class="pl-ii">$</span>lower, df_data<span class="pl-ii">$</span>exposure)<span class="pl-ii">$</span>y
pred_upper <span class="pl-k">&lt;</span><span class="pl-k">-</span> approx(df_compressed<span class="pl-ii">$</span>exposure, df_compressed<span class="pl-ii">$</span>upper, df_data<span class="pl-ii">$</span>exposure)<span class="pl-ii">$</span>y

df_data[, <span class="pl-bu">`:=`</span>(pred_lower = pred_lower, pred_upper = pred_upper)]
df_data <span class="pl-k">&lt;</span><span class="pl-k">-</span> df_data[, <span class="pl-k">is</span> := ifelse((mean <span class="pl-k">&gt;=</span> pred_lower) <span class="pl-k">&amp;</span> (mean <span class="pl-k">&lt;=</span> pred_upper), <span class="pl-c1">1</span>, <span class="pl-c1">0</span>)]

is_coverage <span class="pl-k">&lt;</span><span class="pl-k">-</span> <span class="pl-c1">sum</span>(df_data<span class="pl-ii">$</span><span class="pl-k">is</span>, na.rm<span class="pl-k">=</span>T)<span class="pl-k">/</span>dim(df_data)[<span class="pl-c1">1</span>]
<span class="pl-c1">print</span>(paste0(<span class="pl-s"><span class="pl-pds">"</span>In-sample coverage: <span class="pl-pds">"</span></span>, is_coverage))

<span class="pl-c"><span class="pl-c">#</span>Save to PDF</span>
pdf(<span class="pl-s"><span class="pl-pds">"</span>FILEPATH<span class="pl-pds">"</span></span>)
<span class="pl-c1">print</span>(plot)      
dev.off()</pre></div>
</article>
  </div>

  </div>

  <details class="details-reset details-overlay details-overlay-dark">
    <summary data-hotkey="l" aria-label="Jump to line"></summary>
    <details-dialog class="Box Box--overlay d-flex flex-column anim-fade-in fast linejump" aria-label="Jump to line">
      <!-- '"` --><!-- </textarea></xmp> --></option></form><form class="js-jump-to-line-form Box-body d-flex" action="" accept-charset="UTF-8" method="get"><input name="utf8" type="hidden" value="&#x2713;" />
        <input class="form-control flex-auto mr-3 linejump-input js-jump-to-line-field" type="text" placeholder="Jump to line&hellip;" aria-label="Jump to line" autofocus>
        <button type="submit" class="btn" data-close-dialog>Go</button>
</form>    </details-dialog>
  </details>


  </div>
  <div class="modal-backdrop js-touch-events"></div>
</div>

    </div>
  </div>

  </div>

        
<div class="footer container-lg px-3" role="contentinfo">
  <div class="position-relative d-flex flex-justify-between pt-6 pb-2 mt-6 f6 text-gray border-top border-gray-light ">
    <ul class="list-style-none d-flex flex-wrap ">
      <li class="mr-3">&copy; 2018 <span title="0.22401s from unicorn-54b5696fbb-4rf22">GitHub</span>, Inc.</li>
        <li class="mr-3"><a data-ga-click="Footer, go to terms, text:terms" href="https://github.com/site/terms">Terms</a></li>
        <li class="mr-3"><a data-ga-click="Footer, go to privacy, text:privacy" href="https://github.com/site/privacy">Privacy</a></li>
        <li class="mr-3"><a href="https://help.github.com/articles/github-security/" data-ga-click="Footer, go to security, text:security">Security</a></li>
        <li class="mr-3"><a href="https://status.github.com/" data-ga-click="Footer, go to status, text:status">Status</a></li>
        <li><a data-ga-click="Footer, go to help, text:help" href="https://help.github.com">Help</a></li>
    </ul>

    <a aria-label="Homepage" title="GitHub" class="footer-octicon mr-lg-4" href="https://github.com">
      <svg height="24" class="octicon octicon-mark-github" viewBox="0 0 16 16" version="1.1" width="24" aria-hidden="true"><path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"/></svg>
</a>
   <ul class="list-style-none d-flex flex-wrap ">
        <li class="mr-3"><a data-ga-click="Footer, go to contact, text:contact" href="https://github.com/contact">Contact GitHub</a></li>
        <li class="mr-3"><a href="https://github.com/pricing" data-ga-click="Footer, go to Pricing, text:Pricing">Pricing</a></li>
      <li class="mr-3"><a href="https://developer.github.com" data-ga-click="Footer, go to api, text:api">API</a></li>
      <li class="mr-3"><a href="https://training.github.com" data-ga-click="Footer, go to training, text:training">Training</a></li>
        <li class="mr-3"><a href="https://blog.github.com" data-ga-click="Footer, go to blog, text:blog">Blog</a></li>
        <li><a data-ga-click="Footer, go to about, text:about" href="https://github.com/about">About</a></li>

    </ul>
  </div>
  <div class="d-flex flex-justify-center pb-6">
    <span class="f6 text-gray-light"></span>
  </div>
</div>



  <div id="ajax-error-message" class="ajax-error-message flash flash-error">
    <svg class="octicon octicon-alert" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M8.893 1.5c-.183-.31-.52-.5-.887-.5s-.703.19-.886.5L.138 13.499a.98.98 0 0 0 0 1.001c.193.31.53.501.886.501h13.964c.367 0 .704-.19.877-.5a1.03 1.03 0 0 0 .01-1.002L8.893 1.5zm.133 11.497H6.987v-2.003h2.039v2.003zm0-3.004H6.987V5.987h2.039v4.006z"/></svg>
    <button type="button" class="flash-close js-ajax-error-dismiss" aria-label="Dismiss error">
      <svg class="octicon octicon-x" viewBox="0 0 12 16" version="1.1" width="12" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M7.48 8l3.75 3.75-1.48 1.48L6 9.48l-3.75 3.75-1.48-1.48L4.52 8 .77 4.25l1.48-1.48L6 6.52l3.75-3.75 1.48 1.48L7.48 8z"/></svg>
    </button>
    You can’t perform that action at this time.
  </div>


    
    <script crossorigin="anonymous" integrity="sha512-j7P2Pw3104HznNqyNm7WuCF8Lstcf/sPX5meP6e5RFF177kmi6SAbkZ52A3ttKj0cRHLRrUbk7C1w1xtwh52zA==" type="application/javascript" src="https://assets-cdn.github.com/assets/frameworks-c163002918ede72971a36e0025f67a4a.js"></script>
    
    <script crossorigin="anonymous" async="async" integrity="sha512-q+4fkDjLtfp4SakrROelEf5243DwVcEpAt7QGhEwAe99LKRzHcsMS99wjZ1GuM+FvKUKtV3tBtTSGsZwhOqcAw==" type="application/javascript" src="https://assets-cdn.github.com/assets/github-a9c05a8dd8e1c3d54c57deb2eceae349.js"></script>
    
    
    
  <div class="js-stale-session-flash stale-session-flash flash flash-warn flash-banner d-none">
    <svg class="octicon octicon-alert" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M8.893 1.5c-.183-.31-.52-.5-.887-.5s-.703.19-.886.5L.138 13.499a.98.98 0 0 0 0 1.001c.193.31.53.501.886.501h13.964c.367 0 .704-.19.877-.5a1.03 1.03 0 0 0 .01-1.002L8.893 1.5zm.133 11.497H6.987v-2.003h2.039v2.003zm0-3.004H6.987V5.987h2.039v4.006z"/></svg>
    <span class="signed-in-tab-flash">You signed in with another tab or window. <a href="">Reload</a> to refresh your session.</span>
    <span class="signed-out-tab-flash">You signed out in another tab or window. <a href="">Reload</a> to refresh your session.</span>
  </div>
  <div class="facebox" id="facebox" style="display:none;">
  <div class="facebox-popup">
    <div class="facebox-content" role="dialog" aria-labelledby="facebox-header" aria-describedby="facebox-description">
    </div>
    <button type="button" class="facebox-close js-facebox-close" aria-label="Close modal">
      <svg class="octicon octicon-x" viewBox="0 0 12 16" version="1.1" width="12" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M7.48 8l3.75 3.75-1.48 1.48L6 9.48l-3.75 3.75-1.48-1.48L4.52 8 .77 4.25l1.48-1.48L6 6.52l3.75-3.75 1.48 1.48L7.48 8z"/></svg>
    </button>
  </div>
</div>

  <template id="site-details-dialog">
  <details class="details-reset details-overlay details-overlay-dark lh-default text-gray-dark" open>
    <summary aria-haspopup="dialog" aria-label="Close dialog"></summary>
    <details-dialog class="Box Box--overlay d-flex flex-column anim-fade-in fast">
      <button class="Box-btn-octicon m-0 btn-octicon position-absolute right-0 top-0" type="button" aria-label="Close dialog" data-close-dialog>
        <svg class="octicon octicon-x" viewBox="0 0 12 16" version="1.1" width="12" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M7.48 8l3.75 3.75-1.48 1.48L6 9.48l-3.75 3.75-1.48-1.48L4.52 8 .77 4.25l1.48-1.48L6 6.52l3.75-3.75 1.48 1.48L7.48 8z"/></svg>
      </button>
      <div class="octocat-spinner my-6 js-details-dialog-spinner"></div>
    </details-dialog>
  </details>
</template>

  <div class="Popover js-hovercard-content position-absolute" style="display: none; outline: none;" tabindex="0">
  <div class="Popover-message Popover-message--bottom-left Popover-message--large Box box-shadow-large" style="width:360px;">
  </div>
</div>

<div id="hovercard-aria-description" class="sr-only">
  Press h to open a hovercard with more details.
</div>


  </body>
</html>

