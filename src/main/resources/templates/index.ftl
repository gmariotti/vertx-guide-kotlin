<#include "header.ftl">

<div class="row">

    <div class="col-md-12 mt-1">
        <div class="float-xs-right">
            <form class="form-inline" action="/create" method="post">
                <div class="form-group">
                    <input type="text" class="form-control" id="name" name="name" placeholder="New page name">
                </div>
                <button type="submit" class="btn btn-primary">Create</button>
            </form>
        </div>
        <h1 class="display-4">${context.title}</h1>
    </div>

    <div class="col-md-12 mt-1">
  <#list context.pages>
    <h2>Pages:</h2>
    <ul>
      <#items as page>
          <li><a href="/wiki/${page}">${page}</a></li>
      </#items>
    </ul>
  <#else>
    <p>The wiki is currently empty!</p>
  </#list>
  <#if context.backup_gist_url?has_content>
    <div class="alert alert-success" role="alert">
        Successfully created a backup:
        <a href="${context.backup_gist_url}" class="alert-link">${context.backup_gist_url}</a>
    </div>
  <#else>
    <p>
        <a class="btn btn-outline-secondary btn-sm" href="/backup" role="button" aria-pressed="true">Backup</a>
    </p>
  </#if>
    </div>

</div>

<#include "footer.ftl">