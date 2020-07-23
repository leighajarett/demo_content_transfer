'''
This Cloud function is responsible for:
-Creating a new repo + migrating git content
-Creating a new project if applicable
-Creating / syncing dashboards
-Creating / syncing boards
'''


import looker_sdk
import os
import re
import requests
import json
import os.path
from os import path
import github
import time
 

def get_metadata(hosts_short):
    os.environ['LOOKERSDK_BASE_URL']='https://googledemo.looker.com'+str(':19999')
    os.environ['LOOKERSDK_CLIENT_ID']=os.environ.get('GOOGLEDEMO_CLIENT_ID')
    os.environ['LOOKERSDK_CLIENT_SECRET']=os.environ.get('GOOGLEDEMO_CLIENT_SECRET')
    sdk = looker_sdk.init31()

    #repo name
    repos = json.loads(sdk.run_look('73',result_format='json'))
    repo_name = [r['core_demos.development_git'] for r in repos if r['core_demos.lookml_project_name'] == project_name ][0]

    #initialize spaces dict, which tells if we need to import into that host
    spaces = {}
    for h in hosts_short:
        spaces[h]=-1
    dashboards = json.loads(sdk.run_look('44',result_format='json'))
    dashboards_dict = {}
    for dash in dashboards:
        if dash['core_demos.lookml_project_name'] == project_name:
            dashboards_dict[dash['demo_dashboards.development_dashboard_id']] = dash
            for h in hosts_short:
                if dash['demo_dashboards.' + h] is not None:
                    spaces[h] += 1

    #get boardsmetadata
    dashboards_ = json.loads(sdk.run_look('47',result_format='json'))
    dashboards_board_dict = {}
    for dash in dashboards_:
        if dash['core_demos.lookml_project_name'] == project_name:
            for h in hosts_short:
                if dash['demo_use_cases.{}_board'.format(h)] is not None:  
                    if dash['demo_dashboards.development_dashboard_id'] in dashboards_board_dict.keys():
                        if 'trial' in dashboards_board_dict[dash['demo_dashboards.development_dashboard_id']].keys():
                            dashboards_board_dict[dash['demo_dashboards.development_dashboard_id']][h].append(dash) 
                        else:
                            dashboards_board_dict[dash['demo_dashboards.development_dashboard_id']][h] = [dash]
                    else:
                        dashboards_board_dict[dash['demo_dashboards.development_dashboard_id']]=dict()
                        dashboards_board_dict[dash['demo_dashboards.development_dashboard_id']][h] = [dash]
    
    #get models metadata
    models__ = json.loads(sdk.run_look('75',result_format='json'))
    models_ = []
    for m in models__:
        if m['core_demos.lookml_project_name'] == project_name:
            models_.append(m['demo_dashboards.model'])


    return dashboards_dict, dashboards_board_dict, spaces, repo_name, models_

def recursive_delete(path, new_repo):
    contents = new_repo.get_contents(path, ref="master")
    if isinstance(contents,list):
        for c_file in contents:
            recursive_delete(c_file.path,new_repo)
    else:
        new_repo.delete_file(contents.path, "remove workflows", contents.sha, branch="master")
    return
    
def create_project(host_url, sdk, is_demo, repo_name):
    print('Project Does Not Exist, creating project')
    sdk.update_session(looker_sdk.models.WriteApiSession(workspace_id="dev"))
    try: 
        #see if project already exists

        proj = sdk.project(project_name)
        try:
            #try using existing key
            key = sdk.git_deploy_key(proj.id)
        except:
            #otherwise create a brand new key
            key = sdk.create_git_deploy_key(proj.id)
    except:
        #if project doesnt exist then create it and a key
        try:
            proj = sdk.create_project(looker_sdk.models.WriteProject(name=project_name))
            key = sdk.create_git_deploy_key(proj.id)
        except:
            print('Cant create project / connect to git for', host_url)
            return 1
    
    demo_repo = g.get_organization(repo_name.split('/')[1]).get_repo(repo_name.split('/')[2])

    if is_demo:
        try:
            #add the key to the demo repo
            demo_repo.create_key(title='Looker Deploy Key',key=key)
            #connect git to demo repo
            sdk.update_project(proj.id, looker_sdk.models.WriteProject(git_remote_url=demo_repo.ssh_url))
        except:
            print('Cant create project / connect to git for', host_url)
            return 1

    else:
        #if its not a demo instance then create a new repo 
        new_repo_name = project_name + host_url.split('//')[1].split('.')[0] + '_trial'
        try:
            new_repo = g.get_organization('llooker').create_repo_from_template(new_repo_name, demo_repo)
            time.sleep(5)
            recursive_delete(".github", new_repo)
            # demo_repo.create_issue(title=new_repo_name, labels=["trial_lookml"])
            new_repo.create_key(title='Looker Deploy Key',key=key)
            sdk.update_project(proj.id, looker_sdk.models.WriteProject(git_remote_url=new_repo.ssh_url))
        except:
            print('Cant create project / connect to git for', host_url)
            return 1
    
    #run git tests
    git_tests = sdk.all_git_connection_tests(proj.id)
    for i, test in enumerate(git_tests):
        result = sdk.run_git_connection_test(project_id=proj.id, test_id=test.id)
        if result.status != 'pass':
            print('Cant create project / connect to git for', host_url)
            return 1

    try:
        sdk.update_git_branch(proj.id, looker_sdk.models.WriteGitBranch(ref="origin/master"))
    except:
        sdk.create_git_branch(proj.id, looker_sdk.models.WriteGitBranch(name="initiate_remote",ref="origin/master"))
    
    sdk.deploy_to_production(proj.id)
    #hit deploy again
    response = requests.post(url = '{}/webhooks/projects/{}/deploy'.format(host_url,project_name))

    #configure models
    print('Configuring the models')
    for m in models_:
        try:
            sdk.create_lookml_model(looker_sdk.models.WriteLookmlModel(project_name=project_name,name=m, unlimited_db_connections=True))
        except:
            sdk.update_lookml_model(m, looker_sdk.models.WriteLookmlModel(project_name=project_name,unlimited_db_connections=True))

    print("Created new repo: ", new_repo)
    return 0

def sync_content(host_url, host_name, client_id, client_secret, is_demo):
    failed=-1
    os.environ['LOOKERSDK_BASE_URL']=host_url+str(':19999')
    os.environ['LOOKERSDK_CLIENT_ID']=client_id
    os.environ['LOOKERSDK_CLIENT_SECRET']=client_secret

    sdk = looker_sdk.init31()

    #hit deploy webhook
    response = requests.post(url = '{}/webhooks/projects/{}/deploy'.format(host_url,project_name))

    #if project doesnt exist create a new project and then sync the github repo
    if response.status_code == 404:
        failed += create_project(host_url, sdk, is_demo, repo_name)

    if failed < 0:
        for dash_id in dashboard_ids:
            title = dashboards_dict[dash_id]['demo_dashboards.dashboard_name']

            #lookml dashboards ID are generated from the model + title 
            lookml_dash_id = dashboards_dict[dash_id]['demo_dashboards.lookml_dashboard_id']
            if is_demo:
                space_id = dashboards_dict[dash_id]['demo_dashboards.'+ host_name]
            else:
                folders = sdk.folder_children('1')
                folders = [f for f in folders if f.name == project_name]
                if len(folders) > 0:
                    space_id = folders[0].id
                else:
                    space_id = sdk.create_folder(looker_sdk.models.CreateFolder(name=project_name,parent_id='1')).id
            

            #check to see if the dashboard exists in the space
            exists = 0
            slug = dashboards_dict[dash_id]['demo_dashboards.dashboard_slug']
            for u_dash in sdk.space_dashboards(str(space_id)):
                if u_dash.slug == slug:
                    exists = 1
                    new_dash = u_dash
                    # print(new_dash.id)
            if exists >0:
                print('Dashboard %s already exists, syncing it with LookML' %title)
                try:
                    sdk.sync_lookml_dashboard(lookml_dash_id,looker_sdk.models.WriteDashboard())
                except:
                    try:
                        sdk.update_dashboard(new_dash.id,looker_sdk.models.WriteDashboard(lookml_link_id=lookml_dash_id))
                        sdk.sync_lookml_dashboard(lookml_dash_id,looker_sdk.models.WriteDashboard())
                    except:
                        print('Problem syncing  %s  dashboard, ID: %s' %(title, new_dash.id))
                        failed + =1
            else:
                print('Dashboard %s does not yet exist, creating it in space %s' %(title, str(space_id)))
                new_dash = sdk.import_lookml_dashboard(lookml_dash_id,str(space_id),{})
            
            #set the slug 
            sdk.update_dashboard(str(new_dash.id), looker_sdk.models.WriteDashboard(slug=slug))
            
            #check boards and pin to places list
            #wont create the board if its a demo
            #wont unpin dashboard from other places
            if host_name in dashboards_board_dict[dash_id].keys():
                for i in range(len(dashboards_board_dict[dash_id][host_name])):
                    if is_demo:
                        board_id = dashboards_board_dict[dash_id][host_name][i]['demo_use_cases.{}_board'.format(host_name)].split('/')[-1]
                    else:
                        vertical = dashboards_board_dict[dash_id][host_name][i]['demo_use_cases.vertical']
                        all_boards = sdk.all_homepages()
                        all_boards = [b for b in all_boards if b.title==(str(vertical) + ' Demo')] 
                        if len(all_boards) > 0:
                            board_id = all_boards[0].id
                        else:
                            board_id = sdk.create_homepage(looker_sdk.models.WriteHomepage(title=str(vertical) + ' Demo', description='Board created for housing {} demo content'.format(vertical))).id
                    
                    board = sdk.homepage(str(board_id))
                    
                    board_sections=board.homepage_sections
                    found_section = False
                    found_dash = False
                    for section in board_sections:
                        #check if its the right section
                        if section.title == dashboards_board_dict[dash_id][host_name][i]['demo_use_cases.use_case_name']:
                            found_section = True
                            #update the description if its wrong
                            #print(section.description, dashboards_board_dict[dash_id][host_name][i]['demo_use_cases.use_case_description'])
                            if section.description != dashboards_board_dict[dash_id][host_name][i]['demo_use_cases.use_case_description']:
                                print('Updating the description for ', section.title)
                                sdk.update_homepage_section(section.id, looker_sdk.models.WriteHomepageSection(description=dashboards_board_dict[dash_id][host_name][i]['demo_use_cases.use_case_description']))
                            #check if board is already pinned
                            for board_dash in section.homepage_items:
                                if str(board_dash.dashboard_id) == str(new_dash.id):
                                    found_dash = True
                                    break
                            #otherwise pin it
                            if not found_dash:
                                print('dashboard {} not on use case {}, pinning it'.format(new_dash.id, dashboards_board_dict[dash_id][host_name][i]['demo_use_cases.use_case_name']))
                                sdk.create_homepage_item(looker_sdk.models.WriteHomepageItem(homepage_section_id=section.id,dashboard_id=new_dash.id))
                            break
                    if not found_section:
                        print('use case {} not found, creating it and pinning dashboard {}'.format(dashboards_board_dict[dash_id][host_name][i]['demo_use_cases.use_case_name'], new_dash.id))
                        section = sdk.create_homepage_section(looker_sdk.models.WriteHomepageSection(homepage_id=board.id,title=dashboards_board_dict[dash_id][host_name][i]['demo_use_cases.use_case_name'],
                            description=dashboards_board_dict[dash_id][host_name][i]['demo_use_cases.use_case_description']))
                        sdk.create_homepage_item(looker_sdk.models.WriteHomepageItem(homepage_section_id=section.id, dashboard_id=new_dash.id))

    return failed 

def main(request):
    request_json = request.get_json()
    is_demo = True
    global project_name, repo_name, g, dashboards_dict, dashboards_board_dict, spaces, dashboard_ids, models_
    github_token = os.environ.get('GITHUB_PERSONAL_ACCESS_TOKEN')

    if request_json and 'project_name' in request_json:
        project_name = request_json['project_name'] 
    else:
        raise RuntimeError('Missing Demo Project Name')

    if request_json and 'client_id' in request_json:
        client_id = request_json['client_id'] 

    if request_json and 'client_secret' in request_json:
        client_secret = request_json['client_secret'] 

    if request_json and 'base_url' in request_json:
        base_url = request_json['base_url'] 
        is_demo = False

    g = github.Github(github_token)
    host_urls = ['https://googledemo.looker.com','https://partnerdemo.corp.looker.com','https://trial.looker.com']
    hosts_short = [h.split('//')[1].split('.')[0] for h in host_urls]
    dashboards_dict, dashboards_board_dict, spaces, repo_name, models_ = get_metadata(hosts_short)
    dashboard_ids = dashboards_dict.keys()
    if is_demo:
        for host_url, host_name in zip(host_urls,hosts_short):
            #check if we should import into this host
            if spaces[host_name]>-1:
                print('Bringing project over to ', host_url)
                failed += sync_content(host_url, host_name, 
                        os.environ.get('{}_CLIENT_ID'.format(host_name.upper())), 
                        os.environ.get('{}_CLIENT_SECRET'.format(host_name.upper())), is_demo)
    else:
        print('Bringing project over to ', base_url)
        failed = sync_content(base_url, 'trial', client_id, client_secret, is_demo)
    
    if failed > 0:
        raise RuntimeError('Something went wrong syncing dashboards')
    



